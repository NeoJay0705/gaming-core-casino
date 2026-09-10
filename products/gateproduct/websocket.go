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
	"github.com/NeoJay0705/gaming-core-casino/pkg/dispatcher"
	"github.com/NeoJay0705/gaming-core-casino/pkg/framework"
	"github.com/NeoJay0705/gaming-core-casino/pkg/gatelink"
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

// ClosableWebSocketSession is a WebSocket session whose lifecycle can be
// controlled by the Gate session registry.
type ClosableWebSocketSession interface {
	WebSocketSession
	Close() error
}

type webSocketServerInputs struct {
	dig.In

	Snapshot   config.SourceSnapshot
	Dispatcher *dispatcher.Dispatcher
	GameClient *gatelink.Client
	Sessions   *SessionRegistry
	ServerSend *gateServerSendRuntime `optional:"true"`
	Metrics    *gateMetrics
}

// WebSocketServer owns the Gate player-facing WebSocket listener.
// It is inert when the merged configuration has no websocket section.
type WebSocketServer struct {
	cfg        WebSocketConfig
	enabled    bool
	dispatcher *dispatcher.Dispatcher
	gameClient *gatelink.Client
	registry   *SessionRegistry
	serverSend *gateServerSendRuntime
	metrics    *gateMetrics

	mu       sync.Mutex
	server   *http.Server
	listener net.Listener
	stopping bool
	sessions map[WebSocketConnectionID]*webSocketConnection
	wg       sync.WaitGroup
}

func newGateWebSocketServer(inputs webSocketServerInputs) (*WebSocketServer, error) {
	server := &WebSocketServer{
		sessions:   make(map[WebSocketConnectionID]*webSocketConnection),
		dispatcher: inputs.Dispatcher,
		gameClient: inputs.GameClient,
		registry:   inputs.Sessions,
		serverSend: inputs.ServerSend,
		metrics:    inputs.Metrics,
	}
	if server.dispatcher == nil {
		return nil, errors.New("gate websocket: dispatcher is nil")
	}
	if server.gameClient == nil {
		return nil, errors.New("gate websocket: Game client is nil")
	}
	if server.registry == nil {
		return nil, errors.New("gate websocket: session registry is nil")
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
		if err := session.closeWithReason(closeReasonShutdown); err != nil {
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
	session := newWebSocketConnection(conn, s.cfg.WriteChanSize, time.Duration(s.cfg.WriteTimeoutMs)*time.Millisecond, s.metrics)

	s.mu.Lock()
	if s.stopping || s.server == nil {
		s.mu.Unlock()
		// Upgrade 已經成功，仍需讓該 connection 完成一次 metrics lifecycle，
		// 即使它在停止競速中未能加入 active session map。
		session.markAccepted()
		_ = session.closeWithReason(closeReasonShutdown)
		session.finish()
		return
	}
	s.sessions[session.id] = session
	session.markAccepted()
	s.wg.Add(1)
	s.mu.Unlock()
	go s.serveSession(session)
}

func (s *WebSocketServer) serveSession(session *webSocketConnection) {
	defer s.wg.Done()
	defer s.registry.Remove(session)
	writerDone := make(chan struct{})
	go func() {
		session.writeLoop()
		close(writerDone)
	}()
	ctx := session.Context()
	reason := session.readLoop(ctx, func(packet WebSocketPacket) bool {
		return s.dispatchPacket(ctx, session, packet)
	})
	session.closeWithReason(reason)
	<-writerDone
	session.finish()
	s.mu.Lock()
	delete(s.sessions, session.id)
	s.mu.Unlock()
}

// dispatchPacket protects the Gate transport from a product handler panic.
// Locally registered commands stay in Gate; every other command is forwarded
// to Game through the direct Gate-to-Game channel.
func (s *WebSocketServer) dispatchPacket(ctx context.Context, session *webSocketConnection, packet WebSocketPacket) (ok bool) {
	start := time.Now()
	route, command := gateCommandRouteAndLabel(s.dispatcher, packet.CommandID)
	result := "error"
	if s.metrics != nil {
		s.metrics.websocketCommandsInFlight.Inc()
	}
	defer func() {
		if s.metrics != nil {
			s.metrics.websocketCommandsInFlight.Dec()
			s.metrics.observeCommand(route, command, result, time.Since(start))
		}
		if recovered := recover(); recovered != nil {
			ok = false
			log.Printf("[gate websocket] dispatcher panicked: session=%s command=%d panic=%v", session.ID(), packet.CommandID, recovered)
			_ = session.closeWithReason(closeReasonPanic)
		}
	}()
	if ctx.Err() != nil {
		return false
	}
	source := gatelink.RequestSource{ConnectionID: string(session.ID())}
	if s.serverSend != nil {
		route := s.serverSend.Route()
		source.GateID = route.GateID
		source.ReplyEndpoint = route.ReplyEndpoint
	}
	ctx = WithWebSocketRequestContext(ctx, WebSocketRequestContext{
		Request: gatelink.GateRequestContext{
			Source: source,
		},
		Session: session,
		Packet:  packet,
	})
	handled, err := s.dispatcher.Dispatch(ctx, WebSocketChannel, dispatcher.CommandID(packet.CommandID), packet.Payload)
	if err != nil {
		log.Printf("[gate websocket] dispatcher failed: session=%s command=%d err=%v", session.ID(), packet.CommandID, err)
		if errors.Is(err, ErrLoginRequired) {
			_ = session.closeWithReason(closeReasonLoginRequired)
		} else if errors.Is(err, ErrRoomRequired) {
			_ = session.closeWithReason(closeReasonRoomRequired)
		} else {
			_ = session.closeWithReason(closeReasonHandlerError)
		}
		return false
	}
	if handled {
		result = "success"
		return true
	}
	// Gate cannot inspect Game's dispatcher, so every forwarded command must
	// already belong to a logged-in room before a network call is attempted.
	state, exists := s.registry.State(session.ID())
	if !exists {
		_ = session.closeWithReason(closeReasonLoginRequired)
		return false
	}
	if state.RoomID == "" {
		_ = session.closeWithReason(closeReasonRoomRequired)
		return false
	}
	grpcStart := time.Now()
	if s.metrics != nil {
		s.metrics.gameGRPCInFlight.Inc()
	}
	reply, err := s.gameClient.Forward(ctx, gatelink.Request{
		CommandID: packet.CommandID,
		Payload:   packet.Payload,
	})
	if err != nil {
		if s.metrics != nil {
			s.metrics.gameGRPCInFlight.Dec()
			s.metrics.observeGameGRPC(boundedGRPCCode(err), time.Since(grpcStart))
		}
		log.Printf("[gate websocket] forward to Game failed: session=%s command=%d err=%v", session.ID(), packet.CommandID, err)
		_ = session.closeWithReason(closeReasonForwardError)
		return false
	}
	if s.metrics != nil {
		s.metrics.gameGRPCInFlight.Dec()
		s.metrics.observeGameGRPC(boundedGRPCCode(nil), time.Since(grpcStart))
	}
	if reply != nil {
		receivedAt := time.Now()
		if reply.ExpectedLoginName != "" {
			state, exists := s.registry.State(session.ID())
			if !exists || string(state.LoginName) != reply.ExpectedLoginName {
				if s.metrics != nil {
					s.metrics.observeServerSendRequest(string(serverSendTargetConnection), "ignored")
				}
				log.Printf("[gate websocket] forwarded reply identity mismatch: session=%s command=%d", session.ID(), reply.CommandID)
				_ = session.closeWithReason(closeReasonForwardError)
				return false
			}
		}
		if err := sendOutbound(session, outboundMessage{
			data:       encodeWebSocketPacket(WebSocketPacket{CommandID: reply.CommandID, Payload: reply.Payload}),
			source:     outboundSourceServerSend,
			receivedAt: receivedAt,
			target:     serverSendTargetConnection,
		}); err != nil {
			if s.metrics != nil {
				s.metrics.observeServerSendRequest(string(serverSendTargetConnection), "error")
			}
			log.Printf("[gate websocket] enqueue forwarded reply failed: session=%s command=%d err=%v", session.ID(), reply.CommandID, err)
			_ = session.closeWithReason(closeReasonForwardError)
			return false
		}
		if s.metrics != nil {
			s.metrics.observeServerSendRequest(string(serverSendTargetConnection), "queued")
		}
	}
	result = "success"
	return true
}

// WebSocketRequestContext is the single context carrier for a WebSocket
// command. Request holds transport-neutral Gate metadata; Session is the
// Gate-specific connection used to send a response.
type WebSocketRequestContext struct {
	Request gatelink.GateRequestContext
	Session WebSocketSession
	Packet  WebSocketPacket
}

// GateRequestContext makes WebSocketRequestContext usable by the generic
// Gate-to-Game metadata propagation path.
func (c WebSocketRequestContext) GateRequestContext() gatelink.GateRequestContext { return c.Request }

// WithWebSocketRequestContext attaches one complete WebSocket request carrier.
func WithWebSocketRequestContext(ctx context.Context, requestContext WebSocketRequestContext) context.Context {
	return gatelink.WithRequestContextCarrier(ctx, requestContext)
}

// WebSocketRequestContextFrom returns the complete WebSocket request carrier.
func WebSocketRequestContextFrom(ctx context.Context) (WebSocketRequestContext, bool) {
	carrier, ok := gatelink.RequestContextCarrierFrom(ctx)
	if !ok {
		return WebSocketRequestContext{}, false
	}
	requestContext, ok := carrier.(WebSocketRequestContext)
	return requestContext, ok && !framework.IsNilDependency(requestContext.Session)
}

const (
	closeReasonClientClosed   = "client_closed"
	closeReasonReadError      = "read_error"
	closeReasonInvalidFrame   = "invalid_frame"
	closeReasonLoginRequired  = "login_required"
	closeReasonRoomRequired   = "room_required"
	closeReasonHandlerError   = "handler_error"
	closeReasonForwardError   = "forward_error"
	closeReasonWriteError     = "write_error"
	closeReasonWriteQueueFull = "write_queue_full"
	closeReasonServerClosed   = "server_closed"
	closeReasonShutdown       = "shutdown"
	closeReasonPanic          = "panic"
)

type outboundMessage struct {
	data       []byte
	source     outboundSource
	receivedAt time.Time
	target     serverSendTarget
}

type outboundSession interface {
	sendOutbound(outboundMessage) error
}

func sendOutbound(session WebSocketSession, message outboundMessage) error {
	if session == nil {
		return errors.New("gate websocket: session is nil")
	}
	if sender, ok := session.(outboundSession); ok {
		return sender.sendOutbound(message)
	}
	return session.SendBinary(message.data)
}

type webSocketConnection struct {
	id           WebSocketConnectionID
	conn         *websocket.Conn
	writeCh      chan outboundMessage
	closeCh      chan struct{}
	closeOnce    sync.Once
	finishOnce   sync.Once
	writeTimeout time.Duration
	ctx          context.Context
	cancel       context.CancelFunc
	metrics      *gateMetrics

	stateMu     sync.Mutex
	writeMu     sync.Mutex
	closed      bool
	accepted    bool
	closeReason string
	stream      webSocketPacketStream
}

func newWebSocketConnection(conn *websocket.Conn, queueSize int, writeTimeout time.Duration, metrics ...*gateMetrics) *webSocketConnection {
	var observed *gateMetrics
	if len(metrics) != 0 {
		observed = metrics[0]
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &webSocketConnection{
		id:           newWebSocketConnectionID(),
		conn:         conn,
		writeCh:      make(chan outboundMessage, queueSize),
		closeCh:      make(chan struct{}),
		writeTimeout: writeTimeout,
		ctx:          ctx,
		cancel:       cancel,
		metrics:      observed,
	}
}

func (s *webSocketConnection) ID() WebSocketConnectionID { return s.id }

func (s *webSocketConnection) Context() context.Context { return s.ctx }

func (s *webSocketConnection) SendBinary(data []byte) error {
	return s.sendOutbound(outboundMessage{data: data, source: outboundSourceHandler, receivedAt: time.Now()})
}

func (s *webSocketConnection) sendOutbound(message outboundMessage) error {
	message.data = append([]byte(nil), message.data...)
	if message.receivedAt.IsZero() {
		message.receivedAt = time.Now()
	}
	s.stateMu.Lock()
	if s.closed {
		err := errors.New("gate websocket: connection closed")
		s.stateMu.Unlock()
		if message.source == outboundSourceServerSend {
			s.metrics.observeServerSendDelivery(string(message.target), "error", time.Since(message.receivedAt))
		}
		return err
	}
	select {
	case s.writeCh <- message:
		if s.accepted && s.metrics != nil {
			s.metrics.writeQueueMessages.Inc()
		}
		s.stateMu.Unlock()
		return nil
	default:
		s.stateMu.Unlock()
		if s.metrics != nil {
			s.metrics.writeQueueFull.WithLabelValues(string(message.source)).Inc()
			if message.source == outboundSourceServerSend {
				s.metrics.observeServerSendDelivery(string(message.target), "error", time.Since(message.receivedAt))
			}
		}
		_ = s.closeWithReason(closeReasonWriteQueueFull)
		return errWebSocketWriteQueueFull
	}
}

// Close closes this session and cancels all work derived from its context.
func (s *webSocketConnection) Close() error { return s.closeWithReason(closeReasonServerClosed) }

func encodeWebSocketPacket(packet WebSocketPacket) []byte {
	data := make([]byte, webSocketPacketHeaderSize+len(packet.Payload))
	binary.BigEndian.PutUint32(data[0:4], packet.CommandID)
	binary.BigEndian.PutUint32(data[4:8], uint32(len(data)))
	binary.BigEndian.PutUint32(data[8:12], packet.Sequence)
	binary.BigEndian.PutUint16(data[12:14], packet.Session)
	binary.BigEndian.PutUint16(data[14:16], packet.Version)
	copy(data[webSocketPacketHeaderSize:], packet.Payload)
	return data
}

// EncodeWebSocketPacket 編碼一個完整 binary packet，供 Gate WebSocket
// client 或外部 product handler 使用。
func EncodeWebSocketPacket(packet WebSocketPacket) []byte { return encodeWebSocketPacket(packet) }

func (s *webSocketConnection) closeWithReason(reason string) error {
	if reason == "" {
		reason = closeReasonServerClosed
	}
	var closeErr error
	s.closeOnce.Do(func() {
		s.stateMu.Lock()
		s.closed = true
		s.closeReason = reason
		s.cancel()
		close(s.closeCh)
		s.stateMu.Unlock()
		// Do not wait for a writer mutex here: closing the network connection is
		// what unblocks a write that is already stuck in the kernel.
		if s.conn != nil {
			closeErr = s.conn.Close()
		}
	})
	return closeErr
}

func (s *webSocketConnection) currentCloseReason() string {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.closeReason
}

func (s *webSocketConnection) markAccepted() {
	s.stateMu.Lock()
	if s.accepted {
		s.stateMu.Unlock()
		return
	}
	s.accepted = true
	s.stateMu.Unlock()
	if s.metrics != nil {
		s.metrics.websocketConnections.Inc()
		s.metrics.websocketConnectionsTotal.Inc()
		s.metrics.writeQueueCapacity.Add(float64(cap(s.writeCh)))
	}
}

func (s *webSocketConnection) acceptedForMetrics() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.accepted
}

func (s *webSocketConnection) finish() {
	s.finishOnce.Do(func() {
		for {
			select {
			case message := <-s.writeCh:
				if s.acceptedForMetrics() && s.metrics != nil {
					s.metrics.writeQueueMessages.Dec()
				}
				if message.source == outboundSourceServerSend {
					s.metrics.observeServerSendDelivery(string(message.target), "dropped", time.Since(message.receivedAt))
				}
			default:
				s.stateMu.Lock()
				accepted := s.accepted
				s.accepted = false
				reason := s.closeReason
				s.stateMu.Unlock()
				if accepted && s.metrics != nil {
					s.metrics.websocketConnections.Dec()
					s.metrics.writeQueueCapacity.Sub(float64(cap(s.writeCh)))
					s.metrics.websocketConnectionCloses.WithLabelValues(reasonOrDefault(reason, closeReasonClientClosed)).Inc()
				}
				return
			}
		}
	})
}

func reasonOrDefault(reason, fallback string) string {
	if reason == "" {
		return fallback
	}
	return reason
}

func (s *webSocketConnection) writeLoop() {
	ticker := time.NewTicker(defaultPingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.closeCh:
			return
		case message := <-s.writeCh:
			if s.acceptedForMetrics() && s.metrics != nil {
				s.metrics.writeQueueMessages.Dec()
				s.metrics.websocketWritesInFlight.Inc()
			}
			start := time.Now()
			err := s.writeFrame(websocket.BinaryMessage, message.data)
			result := "success"
			if err != nil {
				result = "error"
			}
			if s.metrics != nil {
				if s.acceptedForMetrics() {
					s.metrics.websocketWritesInFlight.Dec()
				}
				s.metrics.websocketWrites.WithLabelValues(string(message.source), result).Inc()
				s.metrics.websocketWriteDuration.WithLabelValues(string(message.source), result).Observe(time.Since(start).Seconds())
				if message.source == outboundSourceServerSend {
					s.metrics.observeServerSendDelivery(string(message.target), result, time.Since(message.receivedAt))
				}
			}
			if err != nil {
				_ = s.closeWithReason(closeReasonWriteError)
				return
			}
		case <-ticker.C:
			if err := s.writeFrame(websocket.PingMessage, nil); err != nil {
				_ = s.closeWithReason(closeReasonWriteError)
				return
			}
		}
	}
}

func (s *webSocketConnection) writeFrame(messageType int, data []byte) error {
	if s.conn == nil {
		return errors.New("gate websocket: connection is nil")
	}
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

func (s *webSocketConnection) readLoop(ctx context.Context, onPacket func(WebSocketPacket) bool) string {
	_ = s.conn.SetReadDeadline(time.Now().Add(defaultPongTimeout))
	s.conn.SetPongHandler(func(string) error {
		return s.conn.SetReadDeadline(time.Now().Add(defaultPongTimeout))
	})
	for {
		messageType, data, err := s.conn.ReadMessage()
		if err != nil {
			if s.currentCloseReason() == "" {
				log.Printf("[gate websocket] read failed: session=%s err=%v", s.ID(), err)
			}
			if reason := s.currentCloseReason(); reason != "" {
				return reason
			}
			if websocket.IsCloseError(err, websocket.CloseProtocolError, websocket.CloseUnsupportedData, websocket.CloseMessageTooBig) {
				return closeReasonInvalidFrame
			}
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway, websocket.CloseNoStatusReceived) {
				return closeReasonClientClosed
			}
			if errors.Is(err, net.ErrClosed) {
				return closeReasonServerClosed
			}
			return closeReasonReadError
		}
		if messageType != websocket.BinaryMessage {
			continue
		}
		packets, err := s.stream.feed(data)
		if err != nil {
			log.Printf("[gate websocket] invalid frame: session=%s err=%v", s.ID(), err)
			return closeReasonInvalidFrame
		}
		for _, packet := range packets {
			if ctx.Err() != nil {
				return reasonOrDefault(s.currentCloseReason(), closeReasonServerClosed)
			}
			if !onPacket(packet) {
				return reasonOrDefault(s.currentCloseReason(), closeReasonHandlerError)
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
	return WebSocketConnectionID(fmt.Sprintf("generated-%d", time.Now().UnixNano()))
}
