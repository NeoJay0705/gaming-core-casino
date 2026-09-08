package gateproduct

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
	"github.com/gorilla/websocket"
	"go.uber.org/dig"
)

const (
	webSocketPacketHeaderSize = 16
	defaultPingInterval       = 30 * time.Second
	defaultPongTimeout        = 60 * time.Second
	defaultWriteChanSize      = 256
	defaultWriteTimeout       = 5 * time.Second
	maxWebSocketPacketBytes   = 1024 * 1024
)

var errWebSocketWriteQueueFull = errors.New("gate websocket: write queue is full")

// WebSocketConnectionID identifies one accepted player WebSocket connection.
type WebSocketConnectionID string

// WebSocketConfig is the Gate client WebSocket configuration.
type WebSocketConfig struct {
	ClientAddr     string `config:"client_addr" yaml:"client_addr"`
	WriteChanSize  int    `config:"write_chan_size" yaml:"write_chan_size"`
	WriteTimeoutMs int    `config:"write_timeout_ms" yaml:"write_timeout_ms"`
}

// WebSocketPacket is the binary Gate application packet received from a
// player. Its header is wire-compatible with the existing Gate protocol.
type WebSocketPacket struct {
	CommandID uint32
	Sequence  uint32
	Session   uint16
	Version   uint16
	Payload   []byte
}

// WebSocketSession is a single player WebSocket connection. SendBinary queues
// one complete application packet for the connection's sole writer goroutine.
type WebSocketSession interface {
	ID() WebSocketConnectionID
	SendBinary([]byte) error
}

// WebSocketIngress is the future Gate business boundary. The base product
// provides no implementation, so an unconfigured business graph remains a
// transport-only no-op while callers may supply one from a product module.
type WebSocketIngress interface {
	OnConnect(context.Context, WebSocketSession)
	OnPacket(context.Context, WebSocketSession, WebSocketPacket)
	OnDisconnect(context.Context, WebSocketSession)
}

type webSocketServerInputs struct {
	dig.In

	Snapshot config.SourceSnapshot
	Ingress  WebSocketIngress `optional:"true"`
}

// WebSocketServer owns the Gate player-facing WebSocket listener.
// It is inert when the merged configuration has no websocket section.
type WebSocketServer struct {
	cfg     WebSocketConfig
	enabled bool
	ingress WebSocketIngress

	mu       sync.Mutex
	server   *http.Server
	listener net.Listener
	stopping bool
	sessions map[WebSocketConnectionID]*webSocketConnection
	wg       sync.WaitGroup
}

func newGateWebSocketServer(inputs webSocketServerInputs) (*WebSocketServer, error) {
	server := &WebSocketServer{sessions: make(map[WebSocketConnectionID]*webSocketConnection)}
	if !framework.IsNilDependency(inputs.Ingress) {
		server.ingress = inputs.Ingress
	}
	if inputs.Snapshot == nil {
		return nil, fmt.Errorf("gate websocket: config snapshot is nil")
	}
	if !inputs.Snapshot.Has("websocket") {
		return server, nil
	}
	if err := inputs.Snapshot.Bind("websocket", &server.cfg); err != nil {
		return nil, fmt.Errorf("gate websocket: bind config: %w", err)
	}
	server.cfg.ClientAddr = strings.TrimSpace(server.cfg.ClientAddr)
	if server.cfg.ClientAddr == "" {
		return nil, errors.New("gate websocket: client_addr is required when websocket is configured")
	}
	if server.cfg.WriteChanSize < 0 {
		return nil, fmt.Errorf("gate websocket: write_chan_size must be zero or greater")
	}
	if server.cfg.WriteChanSize == 0 {
		server.cfg.WriteChanSize = defaultWriteChanSize
	}
	if server.cfg.WriteTimeoutMs < 0 {
		return nil, fmt.Errorf("gate websocket: write_timeout_ms must be zero or greater")
	}
	if server.cfg.WriteTimeoutMs == 0 {
		server.cfg.WriteTimeoutMs = int(defaultWriteTimeout / time.Millisecond)
	}
	server.enabled = true
	return server, nil
}

func newGateWebSocketHook(server *WebSocketServer) framework.Hook {
	return framework.Hook{
		Name:    "gate-websocket",
		Phase:   framework.PhaseIngress,
		OnStart: server.Start,
		OnStop:  server.Stop,
	}
}

// Start binds the configured listener before returning, so an occupied port
// fails the application lifecycle instead of falsely reaching readiness.
func (s *WebSocketServer) Start(ctx context.Context) error {
	if s == nil {
		return fmt.Errorf("gate websocket: nil server")
	}
	if !s.enabled {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping {
		return errors.New("gate websocket: server is stopped")
	}
	if s.server != nil {
		return errors.New("gate websocket: server already started")
	}
	listener, err := net.Listen("tcp", s.cfg.ClientAddr)
	if err != nil {
		return fmt.Errorf("gate websocket: listen %s: %w", s.cfg.ClientAddr, err)
	}
	server := &http.Server{
		Handler:           http.HandlerFunc(s.handleHTTP),
		ReadHeaderTimeout: 10 * time.Second,
	}
	s.listener = listener
	s.server = server
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		_ = server.Serve(listener)
	}()
	return nil
}

// Stop stops accepting clients, closes every active session, and waits for all
// reader/writer goroutines to finish.
func (s *WebSocketServer) Stop(ctx context.Context) error {
	if s == nil || !s.enabled {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	s.mu.Lock()
	if s.stopping {
		s.mu.Unlock()
		return nil
	}
	s.stopping = true
	server := s.server
	s.server = nil
	listener := s.listener
	s.listener = nil
	sessions := make([]*webSocketConnection, 0, len(s.sessions))
	for _, session := range s.sessions {
		sessions = append(sessions, session)
	}
	s.mu.Unlock()

	var errs []error
	if server != nil {
		if err := server.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs = append(errs, err)
		}
	}
	if listener != nil {
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, err)
		}
	}
	for _, session := range sessions {
		if err := session.close(); err != nil {
			errs = append(errs, err)
		}
	}

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		errs = append(errs, ctx.Err())
	}
	return errors.Join(errs...)
}

// Addr returns the actual listener address. It is useful when ClientAddr uses
// port zero and for transport integration tests.
func (s *WebSocketServer) Addr() string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

func (s *WebSocketServer) handleHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/ws" && !strings.HasPrefix(r.URL.Path, "/ws/tcp/") && !strings.HasPrefix(r.URL.Path, "/wss/tcp/") {
		http.NotFound(w, r)
		return
	}
	upgrader := websocket.Upgrader{
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	conn.SetReadLimit(maxWebSocketPacketBytes)
	session := newWebSocketConnection(conn, s.cfg.WriteChanSize, time.Duration(s.cfg.WriteTimeoutMs)*time.Millisecond)

	s.mu.Lock()
	if s.stopping || s.server == nil {
		s.mu.Unlock()
		_ = session.close()
		return
	}
	s.sessions[session.id] = session
	s.wg.Add(1)
	s.mu.Unlock()
	go s.serveSession(session)
}

func (s *WebSocketServer) serveSession(session *webSocketConnection) {
	defer s.wg.Done()
	writerDone := make(chan struct{})
	go func() {
		session.writeLoop()
		close(writerDone)
	}()
	ctx := session.Context()
	if s.callIngress(ctx, session, "connect", func(ingress WebSocketIngress) {
		ingress.OnConnect(ctx, session)
	}) {
		session.readLoop(ctx, func(packet WebSocketPacket) bool {
			return s.callIngress(ctx, session, "packet", func(ingress WebSocketIngress) {
				ingress.OnPacket(ctx, session, packet)
			})
		})
	}
	_ = session.close()
	<-writerDone
	s.callIngress(ctx, session, "disconnect", func(ingress WebSocketIngress) {
		ingress.OnDisconnect(ctx, session)
	})
	s.mu.Lock()
	delete(s.sessions, session.id)
	s.mu.Unlock()
}

// callIngress prevents an extension module panic from terminating the Gate
// process. A panicking callback closes the affected session and is reported to
// the process log; other sessions continue serving normally.
func (s *WebSocketServer) callIngress(ctx context.Context, session *webSocketConnection, stage string, call func(WebSocketIngress)) (ok bool) {
	if s.ingress == nil {
		return true
	}
	ok = true
	defer func() {
		if recovered := recover(); recovered != nil {
			ok = false
			log.Printf("[gate websocket] ingress %s panicked: session=%s panic=%v", stage, session.ID(), recovered)
			_ = session.close()
		}
	}()
	if ctx.Err() != nil && stage != "disconnect" {
		return false
	}
	call(s.ingress)
	return ok
}

type webSocketConnection struct {
	id           WebSocketConnectionID
	conn         *websocket.Conn
	writeCh      chan []byte
	closeCh      chan struct{}
	closeOnce    sync.Once
	writeTimeout time.Duration
	ctx          context.Context
	cancel       context.CancelFunc

	stateMu sync.Mutex
	writeMu sync.Mutex
	closed  bool
	stream  webSocketPacketStream
}

func newWebSocketConnection(conn *websocket.Conn, queueSize int, writeTimeout time.Duration) *webSocketConnection {
	ctx, cancel := context.WithCancel(context.Background())
	return &webSocketConnection{
		id:           newWebSocketConnectionID(),
		conn:         conn,
		writeCh:      make(chan []byte, queueSize),
		closeCh:      make(chan struct{}),
		writeTimeout: writeTimeout,
		ctx:          ctx,
		cancel:       cancel,
	}
}

func (s *webSocketConnection) ID() WebSocketConnectionID { return s.id }

func (s *webSocketConnection) Context() context.Context { return s.ctx }

func (s *webSocketConnection) SendBinary(data []byte) error {
	copyData := append([]byte(nil), data...)
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed {
		return errors.New("gate websocket: connection closed")
	}
	select {
	case s.writeCh <- copyData:
		return nil
	default:
		return errWebSocketWriteQueueFull
	}
}

func (s *webSocketConnection) close() error {
	var closeErr error
	s.closeOnce.Do(func() {
		s.stateMu.Lock()
		s.closed = true
		s.cancel()
		close(s.closeCh)
		s.stateMu.Unlock()
		// Do not wait for a writer mutex here: closing the network connection is
		// what unblocks a write that is already stuck in the kernel.
		closeErr = s.conn.Close()
	})
	return closeErr
}

func (s *webSocketConnection) writeLoop() {
	ticker := time.NewTicker(defaultPingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.closeCh:
			return
		case data := <-s.writeCh:
			if err := s.writeFrame(websocket.BinaryMessage, data); err != nil {
				_ = s.close()
				return
			}
		case <-ticker.C:
			if err := s.writeFrame(websocket.PingMessage, nil); err != nil {
				_ = s.close()
				return
			}
		}
	}
}

func (s *webSocketConnection) writeFrame(messageType int, data []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.stateMu.Lock()
	if s.closed {
		s.stateMu.Unlock()
		return errors.New("gate websocket: connection closed")
	}
	s.stateMu.Unlock()
	if err := s.conn.SetWriteDeadline(time.Now().Add(s.writeTimeout)); err != nil {
		return err
	}
	defer s.conn.SetWriteDeadline(time.Time{})
	return s.conn.WriteMessage(messageType, data)
}

func (s *webSocketConnection) readLoop(ctx context.Context, onPacket func(WebSocketPacket) bool) {
	_ = s.conn.SetReadDeadline(time.Now().Add(defaultPongTimeout))
	s.conn.SetPongHandler(func(string) error {
		return s.conn.SetReadDeadline(time.Now().Add(defaultPongTimeout))
	})
	for {
		messageType, data, err := s.conn.ReadMessage()
		if err != nil {
			return
		}
		if messageType != websocket.BinaryMessage {
			continue
		}
		packets, err := s.stream.feed(data)
		if err != nil {
			return
		}
		for _, packet := range packets {
			if ctx.Err() != nil {
				return
			}
			if !onPacket(packet) {
				return
			}
		}
	}
}

type webSocketPacketStream struct{ buffer []byte }

func (s *webSocketPacketStream) feed(data []byte) ([]WebSocketPacket, error) {
	if len(data) == 0 {
		return nil, nil
	}
	s.buffer = append(s.buffer, data...)
	if len(s.buffer) > maxWebSocketPacketBytes {
		return nil, fmt.Errorf("gate websocket: packet stream exceeds %d bytes", maxWebSocketPacketBytes)
	}
	var packets []WebSocketPacket
	for len(s.buffer) >= webSocketPacketHeaderSize {
		size := int(binary.BigEndian.Uint32(s.buffer[4:8]))
		if size < webSocketPacketHeaderSize || size > maxWebSocketPacketBytes {
			return nil, fmt.Errorf("gate websocket: invalid packet size %d", size)
		}
		if len(s.buffer) < size {
			break
		}
		packet := WebSocketPacket{
			CommandID: binary.BigEndian.Uint32(s.buffer[0:4]),
			Sequence:  binary.BigEndian.Uint32(s.buffer[8:12]),
			Session:   binary.BigEndian.Uint16(s.buffer[12:14]),
			Version:   binary.BigEndian.Uint16(s.buffer[14:16]),
			Payload:   append([]byte(nil), s.buffer[webSocketPacketHeaderSize:size]...),
		}
		packets = append(packets, packet)
		s.buffer = s.buffer[size:]
	}
	return packets, nil
}

func newWebSocketConnectionID() WebSocketConnectionID {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err == nil {
		return WebSocketConnectionID(hex.EncodeToString(bytes[:]))
	}
	return WebSocketConnectionID(fmt.Sprintf("fallback-%d", time.Now().UnixNano()))
}
