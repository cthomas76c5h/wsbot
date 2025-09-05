package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const (
	addr               = ":8081"
	wsPath             = "/ws"
	sampleRate         = 8000 // Hz, server TX/RX rate
	bargeInThreshold   = 900.0
	utteranceSilenceMs = 700
	maxUtterMs         = 7000
	idleRepromptSec    = 12

	writeWait  = 10 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = (pongWait * 9) / 10
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  8192,
	WriteBufferSize: 8192,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

type convState int32

const (
	StateInit convState = iota
	StateWaitUser
	StateResponding
	StateClosing
)

type ConversationManager struct {
	conn        *websocket.Conn
	state       atomic.Int32
	createdAt   time.Time
	bargeInCh   chan struct{}
	ttsCancelCh chan struct{}
	finalUttCh  chan struct{}
	stopAllCh   chan struct{}
	wg          sync.WaitGroup

	lastVoiceMu  sync.Mutex
	lastVoiceAt  time.Time
	ttsInFlight  atomic.Bool
	userSpeaking atomic.Bool
}

func NewConversationManager(conn *websocket.Conn) *ConversationManager {
	cm := &ConversationManager{
		conn:        conn,
		bargeInCh:   make(chan struct{}, 1),
		ttsCancelCh: make(chan struct{}),
		finalUttCh:  make(chan struct{}, 8),
		stopAllCh:   make(chan struct{}),
		createdAt:   time.Now(),
	}
	cm.state.Store(int32(StateInit))
	cm.touchVoice(false)
	return cm
}

func (cm *ConversationManager) touchVoice(voice bool) {
	cm.lastVoiceMu.Lock()
	defer cm.lastVoiceMu.Unlock()
	if voice {
		cm.lastVoiceAt = time.Now()
	} else if cm.lastVoiceAt.IsZero() {
		cm.lastVoiceAt = time.Now()
	}
}

func (cm *ConversationManager) lastVoiceAgo() time.Duration {
	cm.lastVoiceMu.Lock()
	defer cm.lastVoiceMu.Unlock()
	if cm.lastVoiceAt.IsZero() {
		return time.Since(cm.createdAt)
	}
	return time.Since(cm.lastVoiceAt)
}

func (cm *ConversationManager) HandleConnection() {
	log.Printf("client connected")

	cm.wg.Add(3)
	go func() { defer cm.wg.Done(); cm.readMicLoop() }()
	go func() { defer cm.wg.Done(); cm.pingLoop() }()
	go func() { defer cm.wg.Done(); cm.conversationLoop() }()

	cm.wg.Wait()
	_ = cm.conn.Close()
	log.Printf("client disconnected")
}

func (cm *ConversationManager) pingLoop() {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_ = cm.conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(writeWait))
		case <-cm.stopAllCh:
			return
		}
	}
}

// -------- Inbound mic (binary int16 @ 8kHz) + VAD + barge-in --------

func (cm *ConversationManager) readMicLoop() {
	defer close(cm.stopAllCh)

	_ = cm.conn.SetReadDeadline(time.Now().Add(pongWait))
	cm.conn.SetPongHandler(func(string) error {
		_ = cm.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	type vadState int
	const (
		vadSilent vadState = iota
		vadVoice
	)
	state := vadSilent
	voiceStart := time.Time{}
	lastVoiceSeen := time.Time{}

	for {
		mt, payload, err := cm.conn.ReadMessage()
		if err != nil {
			log.Printf("read error: %v", err)
			return
		}
		if mt == websocket.TextMessage {
			continue
		}
		if mt != websocket.BinaryMessage {
			continue
		}

		ints := bytesToInt16(payload)
		if len(ints) == 0 {
			continue
		}

		avg := averageAbs(ints)
		now := time.Now()
		isVoice := avg >= bargeInThreshold
		if isVoice {
			lastVoiceSeen = now
		}
		cm.touchVoice(isVoice)

		// Barge-in if user speaks while TTS is streaming
		if isVoice && cm.ttsInFlight.Load() {
			select {
			case cm.bargeInCh <- struct{}{}:
			default:
			}
			_ = cm.conn.WriteControl(websocket.TextMessage, []byte("STOP"), time.Now().Add(writeWait))
		}

		switch state {
		case vadSilent:
			if isVoice {
				state = vadVoice
				voiceStart = now
				lastVoiceSeen = now
				cm.userSpeaking.Store(true)
			}
		case vadVoice:
			if !isVoice && !lastVoiceSeen.IsZero() &&
				now.Sub(lastVoiceSeen) > time.Duration(utteranceSilenceMs)*time.Millisecond {
				state = vadSilent
				cm.userSpeaking.Store(false)
				select {
				case cm.finalUttCh <- struct{}{}:
				default:
				}
			}
			if now.Sub(voiceStart) > time.Duration(maxUtterMs)*time.Millisecond {
				state = vadSilent
				cm.userSpeaking.Store(false)
				select {
				case cm.finalUttCh <- struct{}{}:
				default:
				}
			}
		}
	}
}

func bytesToInt16(b []byte) []int16 {
	if len(b)%2 != 0 {
		return nil
	}
	out := make([]int16, len(b)/2)
	_ = binary.Read(bytes.NewReader(b), binary.LittleEndian, &out)
	return out
}

func averageAbs(x []int16) float64 {
	if len(x) == 0 {
		return 0
	}
	var sum float64
	for _, v := range x {
		if v < 0 {
			sum -= float64(v)
		} else {
			sum += float64(v)
		}
	}
	return sum / float64(len(x))
}

// -------------------- Conversation FSM + TTS --------------------

func (cm *ConversationManager) conversationLoop() {
	defer cm.cancelTTS()

	// Greeting
	cm.state.Store(int32(StateResponding))
	cm.playPromptCancelable("greeting")
	cm.state.Store(int32(StateWaitUser))

	next := 0
	script := []string{"ask_name", "ack", "ask_reason", "goodbye"}

	idleTick := time.NewTicker(1 * time.Second)
	defer idleTick.Stop()

	for {
		select {
		case <-cm.finalUttCh:
			if next >= len(script) {
				cm.state.Store(int32(StateClosing))
				return
			}
			cm.state.Store(int32(StateResponding))
			cm.playPromptCancelable(script[next])
			next++
			if next >= len(script) {
				cm.state.Store(int32(StateClosing))
				return
			}
			cm.state.Store(int32(StateWaitUser))

		case <-cm.bargeInCh:
			cm.cancelTTS()

		case <-idleTick.C:
			if convState(cm.state.Load()) == StateWaitUser &&
				cm.lastVoiceAgo() > time.Duration(idleRepromptSec)*time.Second {
				cm.playPromptCancelable("reprompt")
				cm.touchVoice(false)
			}

		case <-cm.stopAllCh:
			return
		}
	}
}

func (cm *ConversationManager) cancelTTS() {
	select {
	case <-cm.ttsCancelCh:
	default:
	}
	close(cm.ttsCancelCh)
	cm.ttsCancelCh = make(chan struct{})
}

func (cm *ConversationManager) playPromptCancelable(name string) {
	samples, err := loadPrompt(name)
	if err != nil || len(samples) == 0 {
		samples = synthFallback(name)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		select {
		case <-cm.ttsCancelCh:
			cancel()
		case <-done:
		}
	}()
	defer close(done)

	cm.ttsInFlight.Store(true)
	defer cm.ttsInFlight.Store(false)

	if cm.userSpeaking.Load() {
		return
	}

	if err := cm.writePCMClip(ctx, samples); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("tts stream error: %v", err)
	}
}

// 20 ms frames @ 8kHz; cancel mid-stream
func (cm *ConversationManager) writePCMClip(ctx context.Context, pcm []int16) error {
	cm.conn.SetWriteDeadline(time.Now().Add(writeWait))

	frames := sliceFrames(pcm, 160) // 160 samples == 20ms @ 8k
	frameDur := time.Duration(float64(time.Second) * 160.0 / float64(sampleRate))

	for _, fr := range frames {
		select {
		case <-ctx.Done():
			return context.Canceled
		default:
		}
		buf := new(bytes.Buffer)
		if err := binary.Write(buf, binary.LittleEndian, fr); err != nil {
			return err
		}
		if err := cm.conn.WriteMessage(websocket.BinaryMessage, buf.Bytes()); err != nil {
			return err
		}
		time.Sleep(frameDur)
	}
	return nil
}

func sliceFrames(pcm []int16, frameSamples int) [][]int16 {
	if frameSamples <= 0 {
		frameSamples = 160
	}
	var out [][]int16
	for i := 0; i < len(pcm); i += frameSamples {
		end := i + frameSamples
		if end > len(pcm) {
			end = len(pcm)
		}
		chunk := make([]int16, end-i)
		copy(chunk, pcm[i:end])
		out = append(out, chunk)
	}
	return out
}

// -------------------- Prompt loading / WAV handling --------------------

func loadPrompt(name string) ([]int16, error) {
	path := filepath.Join("prompts", name+".wav")
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pcm16, sr, ch, bps, err := parseWAV(b)
	if err != nil {
		return nil, err
	}
	if bps != 16 {
		return nil, errors.New("only 16-bit PCM supported")
	}
	// channels -> mono float
	srcMono := toMonoFloat32(pcm16, ch)
	// resample to 8k if needed
	var f8k []float32
	if sr != sampleRate {
		f8k = resampleLinear(srcMono, sr, sampleRate)
	} else {
		f8k = srcMono
	}
	// float -> int16
	out := make([]int16, len(f8k))
	for i, v := range f8k {
		if v > 1 {
			v = 1
		}
		if v < -1 {
			v = -1
		}
		out[i] = int16(v * 32767)
	}
	log.Printf("prompt %s.wav: %d->%d Hz, %dch -> mono, %d samples",
		name, sr, sampleRate, ch, len(out))
	return out, nil
}

// Minimal WAV parser (RIFF/WAVE, PCM16)
func parseWAV(b []byte) ([]int16, int, int, int, error) {
	if len(b) < 44 {
		return nil, 0, 0, 0, errors.New("wav too small")
	}
	r := bytes.NewReader(b)
	var riff [4]byte
	var size uint32
	var wave [4]byte
	if err := binary.Read(r, binary.LittleEndian, &riff); err != nil {
		return nil, 0, 0, 0, err
	}
	if err := binary.Read(r, binary.LittleEndian, &size); err != nil {
		return nil, 0, 0, 0, err
	}
	if err := binary.Read(r, binary.LittleEndian, &wave); err != nil {
		return nil, 0, 0, 0, err
	}
	if string(riff[:]) != "RIFF" || string(wave[:]) != "WAVE" {
		return nil, 0, 0, 0, errors.New("not RIFF/WAVE")
	}

	var (
		audioFormat uint16
		numCh       uint16
		sr          uint32
		bps         uint16
	)
	var data []byte

	for {
		var chunk [4]byte
		var csize uint32
		if err := binary.Read(r, binary.LittleEndian, &chunk); err != nil {
			break
		}
		if err := binary.Read(r, binary.LittleEndian, &csize); err != nil {
			return nil, 0, 0, 0, err
		}
		switch string(chunk[:]) {
		case "fmt ":
			fmtbuf := make([]byte, csize)
			if _, err := r.Read(fmtbuf); err != nil {
				return nil, 0, 0, 0, err
			}
			br := bytes.NewReader(fmtbuf)
			if err := binary.Read(br, binary.LittleEndian, &audioFormat); err != nil {
				return nil, 0, 0, 0, err
			}
			if err := binary.Read(br, binary.LittleEndian, &numCh); err != nil {
				return nil, 0, 0, 0, err
			}
			if err := binary.Read(br, binary.LittleEndian, &sr); err != nil {
				return nil, 0, 0, 0, err
			}
			// skip byte rate (4) + block align (2)
			if _, err := br.Seek(6, io.SeekCurrent); err != nil {
				return nil, 0, 0, 0, err
			}
			if err := binary.Read(br, binary.LittleEndian, &bps); err != nil {
				return nil, 0, 0, 0, err
			}
		case "data":
			data = make([]byte, csize)
			if _, err := r.Read(data); err != nil {
				return nil, 0, 0, 0, err
			}
		default:
			if _, err := r.Seek(int64(csize), io.SeekCurrent); err != nil {
				return nil, 0, 0, 0, err
			}
		}
	}

	if audioFormat != 1 {
		return nil, 0, 0, 0, errors.New("compressed WAV not supported")
	}
	if len(data) == 0 {
		return nil, 0, 0, 0, errors.New("no data chunk")
	}
	if bps != 16 {
		return nil, 0, 0, 0, errors.New("only 16-bit supported")
	}

	pcm := make([]int16, len(data)/2)
	if err := binary.Read(bytes.NewReader(data), binary.LittleEndian, &pcm); err != nil {
		return nil, 0, 0, 0, err
	}
	return pcm, int(sr), int(numCh), int(bps), nil
}

// Mix any channel count to mono float32 [-1,1]
func toMonoFloat32(pcm []int16, channels int) []float32 {
	if channels <= 1 {
		out := make([]float32, len(pcm))
		for i, s := range pcm {
			out[i] = float32(s) / 32768
		}
		return out
	}
	n := len(pcm) / channels
	out := make([]float32, n)
	for i := 0; i < n; i++ {
		var sum float32
		for c := 0; c < channels; c++ {
			sum += float32(pcm[i*channels+c]) / 32768
		}
		out[i] = sum / float32(channels)
	}
	return out
}

// Linear resampling
func resampleLinear(src []float32, srcRate, dstRate int) []float32 {
	if srcRate == dstRate || len(src) == 0 {
		cp := make([]float32, len(src))
		copy(cp, src)
		return cp
	}
	ratio := float64(dstRate) / float64(srcRate)
	outN := int(math.Ceil(float64(len(src)) * ratio))
	out := make([]float32, outN)
	for i := 0; i < outN; i++ {
		t := float64(i) / ratio
		i0 := int(math.Floor(t))
		if i0 >= len(src)-1 {
			out[i] = src[len(src)-1]
			continue
		}
		i1 := i0 + 1
		alpha := float32(t - float64(i0))
		out[i] = src[i0]*(1-alpha) + src[i1]*alpha
	}
	return out
}

// Tone fallbacks if prompts missing
func synthFallback(name string) []int16 {
	switch name {
	case "greeting":
		return concat(tone(550, 250), silence(120), tone(660, 280))
	case "ack":
		return concat(tone(880, 120))
	case "ask_name":
		return concat(tone(700, 180), silence(100), tone(900, 180))
	case "ask_reason":
		return concat(tone(700, 180), silence(100), tone(700, 180), silence(100), tone(900, 220))
	case "reprompt":
		return concat(tone(420, 120), silence(80), tone(420, 120), silence(80), tone(420, 200))
	case "goodbye":
		return concat(tone(700, 120), silence(80), tone(500, 220))
	default:
		return tone(600, 200)
	}
}

func tone(freqHz float64, ms int) []int16 {
	n := int(float64(ms) * float64(sampleRate) / 1000.0)
	out := make([]int16, n)
	var phase float64
	inc := 2 * math.Pi * freqHz / float64(sampleRate)
	for i := 0; i < n; i++ {
		v := math.Sin(phase)
		phase += inc
		out[i] = int16(v * 0.4 * 32767)
	}
	return out
}
func silence(ms int) []int16 {
	n := int(float64(ms) * float64(sampleRate) / 1000.0)
	return make([]int16, n)
}
func concat(a ...[]int16) []int16 {
	var total int
	for _, s := range a {
		total += len(s)
	}
	out := make([]int16, 0, total)
	for _, s := range a {
		out = append(out, s...)
	}
	return out
}

// -------------------- HTTP/WS --------------------

func wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		http.Error(w, "upgrade failed", http.StatusBadRequest)
		return
	}
	cm := NewConversationManager(conn)
	cm.HandleConnection()
}

func main() {
	http.HandleFunc(wsPath, wsHandler)
	srv := &http.Server{Addr: addr}
	log.Printf("WebSocket server on ws://localhost%v%v", addr, wsPath)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("server error: %v", err)
	}
}
