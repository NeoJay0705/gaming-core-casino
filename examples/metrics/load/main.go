package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/examples/metrics/internal/protocol"
	"github.com/NeoJay0705/gaming-core-casino/internal/profilehttp"
	"github.com/NeoJay0705/gaming-core-casino/pkg/gateproto"
	"github.com/NeoJay0705/gaming-core-casino/products/gateproduct"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

// 保留 protobuf 與既有 WebSocket header 的空間，讓 request/response 都不
// 超過共用的 1 MiB application packet 上限。
const maxPayloadBytes = 1024*1024 - 32

const (
	defaultSetupTimeout     = 2 * time.Minute
	defaultSetupConcurrency = 32
	defaultWarmupRequests   = 1
	defaultRequestTimeout   = 10 * time.Second
	defaultPushWarmup       = 5 * time.Second
	defaultPushDrain        = 10 * time.Second
)

type echoRoute string

const (
	echoRouteGame  echoRoute = "game"
	echoRouteLocal echoRoute = "local"
)

type echoCommands struct {
	route             echoRoute
	requestCommandID  uint32
	responseCommandID uint32
}

func parseEchoRoute(value string) (echoCommands, error) {
	switch echoRoute(strings.TrimSpace(value)) {
	case echoRouteGame:
		return echoCommands{
			route:             echoRouteGame,
			requestCommandID:  protocol.EchoRequestCommandID,
			responseCommandID: protocol.EchoResponseCommandID,
		}, nil
	case echoRouteLocal:
		return echoCommands{
			route:             echoRouteLocal,
			requestCommandID:  protocol.LocalEchoRequestCommandID,
			responseCommandID: protocol.LocalEchoResponseCommandID,
		}, nil
	default:
		return echoCommands{}, fmt.Errorf("echo-route must be %q or %q", echoRouteGame, echoRouteLocal)
	}
}

type preparedConnection struct {
	conn     *websocket.Conn
	sequence uint32
	name     string
}

func main() {
	gateURL := flag.String("gate-url", "ws://127.0.0.1:18080/ws", "Gate WebSocket URL")
	connections := flag.Int("connections", 1, "number of closed-loop WebSocket connections")
	duration := flag.Duration("duration", 30*time.Second, "load duration")
	payloadBytes := flag.Int("payload-bytes", 32, "application payload size, bounded to the shared 1 MiB packet limit")
	workload := flag.String("workload", "echo", "workload: echo, broadcast, or player")
	pushInterval := flag.Duration("push-interval", 33*time.Millisecond, "push interval: 33ms or 16ms")
	pushWarmup := flag.Duration("push-warmup-duration", defaultPushWarmup, "push warm-up duration")
	drainTimeout := flag.Duration("drain-timeout", defaultPushDrain, "maximum push drain duration")
	orchestrationDir := flag.String("orchestration-dir", "", "example-only Echo/push validation orchestration directory")
	pprofAddr := flag.String("pprof-addr", "", "optional loopback pprof listen address")
	echoRouteValue := flag.String("echo-route", string(echoRouteGame), "Echo route: game or local")
	metricsAddr := flag.String("metrics-addr", "127.0.0.1:22081", "load metrics listen address")
	setupTimeout := flag.Duration("setup-timeout", defaultSetupTimeout, "timeout for WebSocket setup and warm-up")
	setupConcurrency := flag.Int("setup-concurrency", defaultSetupConcurrency, "maximum concurrent WebSocket setup operations")
	warmupRequests := flag.Int("warmup-requests", defaultWarmupRequests, "Echo requests per connection before measurement")
	requestTimeout := flag.Duration("request-timeout", defaultRequestTimeout, "timeout for one Login, EnterRoom, Echo, or push control round trip")
	flag.Parse()
	if *connections <= 0 || *duration <= 0 || *payloadBytes < 0 || *payloadBytes > maxPayloadBytes || *setupTimeout <= 0 || *setupConcurrency <= 0 || *warmupRequests < 0 || *requestTimeout <= 0 {
		log.Fatalf("connections, duration, setup-timeout, setup-concurrency, and request-timeout must be positive; warmup-requests must not be negative; payload-bytes must be between 0 and %d", maxPayloadBytes)
	}
	if !strings.EqualFold(strings.TrimSpace(*workload), "echo") && (*pushWarmup <= 0 || *drainTimeout <= 0) {
		log.Fatalf("push-warmup-duration and drain-timeout must be positive")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	profileServer, profileErr := profilehttp.Start(*pprofAddr)
	if profileErr != nil {
		log.Fatal(profileErr)
	}
	defer func() {
		shutdownContext, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
		defer shutdownCancel()
		if shutdownErr := profileServer.Shutdown(shutdownContext); shutdownErr != nil {
			log.Printf("stop pprof server: %v", shutdownErr)
		}
	}()
	var err error
	if strings.EqualFold(strings.TrimSpace(*workload), "echo") {
		err = runLoadWithOrchestration(ctx, *gateURL, *connections, *duration, *payloadBytes, *metricsAddr, *setupTimeout, *setupConcurrency, *warmupRequests, *requestTimeout, *echoRouteValue, *orchestrationDir)
	} else {
		err = runPushLoad(ctx, *gateURL, *workload, *connections, *duration, *payloadBytes, *metricsAddr, *setupTimeout, *setupConcurrency, *pushWarmup, *drainTimeout, *pushInterval, *requestTimeout, *orchestrationDir)
	}
	if err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}

func runLoad(ctx context.Context, gateURL string, connectionCount int, duration time.Duration, payloadBytes int, metricsAddr string, setupTimeout time.Duration, setupConcurrency int, warmupRequests int, requestTimeout time.Duration, echoRouteValue string) error {
	return runLoadWithOrchestration(ctx, gateURL, connectionCount, duration, payloadBytes, metricsAddr, setupTimeout, setupConcurrency, warmupRequests, requestTimeout, echoRouteValue, "")
}

func runLoadWithOrchestration(ctx context.Context, gateURL string, connectionCount int, duration time.Duration, payloadBytes int, metricsAddr string, setupTimeout time.Duration, setupConcurrency int, warmupRequests int, requestTimeout time.Duration, echoRouteValue, orchestrationDir string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if connectionCount <= 0 || duration <= 0 || payloadBytes < 0 || payloadBytes > maxPayloadBytes || setupTimeout <= 0 || setupConcurrency <= 0 || warmupRequests < 0 || requestTimeout <= 0 {
		return errors.New("invalid load configuration")
	}
	echo, err := parseEchoRoute(echoRouteValue)
	if err != nil {
		return err
	}
	observer, err := newLoadObserver(metricsAddr)
	if err != nil {
		return fmt.Errorf("start load metrics observer: %w", err)
	}
	orchestration, err := newEchoOrchestration(orchestrationDir)
	if err != nil {
		_ = observer.Shutdown(context.Background())
		return err
	}
	prepared := make([]preparedConnection, 0, connectionCount)
	defer func() {
		closePreparedConnections(prepared)
		shutdownContext, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
		if err := observer.Shutdown(shutdownContext); err != nil {
			log.Printf("stop load metrics observer: %v", err)
		}
		shutdownCancel()
	}()

	payload := make([]byte, payloadBytes)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}
	setupStartedAt := time.Now()
	setupContext, setupCancel := context.WithTimeout(ctx, setupTimeout)
	defer setupCancel()
	prepared, err = prepareConnections(setupContext, gateURL, connectionCount, setupConcurrency)
	if err != nil {
		return fmt.Errorf("prepare connections: %w", err)
	}
	if err := initializeConnections(setupContext, prepared, setupConcurrency, requestTimeout); err != nil {
		return fmt.Errorf("initialize connections: %w", err)
	}
	if err := warmupConnections(setupContext, prepared, payload, setupConcurrency, warmupRequests, requestTimeout, echo); err != nil {
		return fmt.Errorf("warm up connections: %w", err)
	}
	setupDuration := time.Since(setupStartedAt)
	if err := orchestration.writeReady(connectionCount, warmupRequests); err != nil {
		return fmt.Errorf("write Echo orchestration ready marker: %w", err)
	}
	if err := orchestration.waitForStart(ctx, setupTimeout); err != nil {
		return fmt.Errorf("wait for Echo orchestration start: %w", err)
	}

	var (
		measurementStart time.Time
		admissionEnd     time.Time
	)

	var (
		wg                 sync.WaitGroup
		successfulRequests atomic.Uint64
		connectionFailures atomic.Uint64
	)
	startMeasured := make(chan struct{})
	for i := range prepared {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-startMeasured
			requests, err := runMeasuredConnection(ctx, &prepared[index], payload, admissionEnd, requestTimeout, echo, observer.metrics)
			successfulRequests.Add(requests)
			if err != nil && !isExpectedLoadTermination(ctx, err) {
				connectionFailures.Add(1)
				log.Printf("load connection failed: name=%s err=%v", prepared[index].name, err)
			}
		}(i)
	}
	measurementStart = time.Now()
	admissionEnd = measurementStart.Add(duration)
	close(startMeasured)
	wg.Wait()
	measurementEnd := time.Now()
	preparedCount := len(prepared)
	if orchestration != nil {
		closePreparedConnections(prepared)
		prepared = nil
		if err := orchestration.writeMeasured(measurementStart, admissionEnd, measurementEnd, successfulRequests.Load(), connectionFailures.Load()); err != nil {
			return fmt.Errorf("write Echo orchestration measured marker: %w", err)
		}
		if err := orchestration.waitForFinalScraped(ctx, setupTimeout); err != nil {
			return fmt.Errorf("wait for Echo orchestration final scrape: %w", err)
		}
	}
	log.Printf("load complete: echo_route=%s connections=%d prepared=%d setup_duration=%s warmup_requests=%d successful_echo_requests=%d connection_failures=%d measurement_start=%s admission_end=%s measurement_end=%s measured_duration=%s", echo.route, connectionCount, preparedCount, setupDuration.Round(time.Millisecond), warmupRequests, successfulRequests.Load(), connectionFailures.Load(), measurementStart.Format(time.RFC3339Nano), admissionEnd.Format(time.RFC3339Nano), measurementEnd.Format(time.RFC3339Nano), measurementEnd.Sub(measurementStart).Round(time.Millisecond))
	return nil
}

// isExpectedLoadTermination 僅將上層 context cancellation 視為正常取消。
// 單筆 request timeout 是 error；正式 duration 到期不會取消已送出的 request。
func isExpectedLoadTermination(ctx context.Context, err error) bool {
	if err == nil || ctx == nil {
		return false
	}
	return errors.Is(ctx.Err(), context.Canceled)
}

func prepareConnections(ctx context.Context, gateURL string, connectionCount int, setupConcurrency int) ([]preparedConnection, error) {
	prepared := make([]preparedConnection, connectionCount)
	runID := time.Now().UnixNano()
	err := runBounded(ctx, connectionCount, setupConcurrency, func(jobContext context.Context, index int) error {
		loginName := fmt.Sprintf("load-%d-%d", runID, index)
		conn, _, err := websocket.DefaultDialer.DialContext(jobContext, gateURL, nil)
		if err != nil {
			if conn != nil {
				_ = conn.Close()
			}
			return err
		}
		if conn == nil {
			return errors.New("dial returned a nil WebSocket connection")
		}
		prepared[index] = preparedConnection{conn: conn, sequence: 1, name: loginName}
		return nil
	})
	if err != nil {
		closePreparedConnections(prepared)
		return nil, err
	}
	return prepared, nil
}

func initializeConnections(ctx context.Context, prepared []preparedConnection, setupConcurrency int, requestTimeout time.Duration) error {
	return runBounded(ctx, len(prepared), setupConcurrency, func(jobContext context.Context, index int) error {
		connection := &prepared[index]
		if err := setupRoundTrip(jobContext, connection.conn, requestTimeout, gateproto.LoginRequestCommandID, &gateproto.LoginRequest{LoginName: connection.name}, gateproto.LoginResponseCommandID, &gateproto.LoginResponse{}, connection.sequence); err != nil {
			return fmt.Errorf("login: %w", err)
		}
		connection.sequence++
		if err := setupRoundTrip(jobContext, connection.conn, requestTimeout, protocol.EnterRoomRequestCommandID, &protocol.EnterRoomRequest{RoomId: "load-room"}, protocol.EnterRoomResponseCommandID, &protocol.EnterRoomResponse{}, connection.sequence); err != nil {
			return fmt.Errorf("enter room: %w", err)
		}
		connection.sequence++
		return nil
	})
}

func warmupConnections(ctx context.Context, prepared []preparedConnection, payload []byte, setupConcurrency int, warmupRequests int, requestTimeout time.Duration, echo echoCommands) error {
	return runBounded(ctx, len(prepared), setupConcurrency, func(jobContext context.Context, index int) error {
		connection := &prepared[index]
		for request := 0; request < warmupRequests; request++ {
			operationContext, cancel := context.WithTimeout(jobContext, requestTimeout)
			err := echoRoundTrip(operationContext, connection.conn, payload, connection.sequence, echo, nil)
			cancel()
			if err != nil {
				return fmt.Errorf("warm-up echo %d: %w", request+1, err)
			}
			connection.sequence++
		}
		return nil
	})
}

func runMeasuredConnection(ctx context.Context, connection *preparedConnection, payload []byte, admissionEnd time.Time, requestTimeout time.Duration, echo echoCommands, metrics *loadMetrics) (uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var requests uint64
	for time.Now().Before(admissionEnd) {
		if err := ctx.Err(); err != nil {
			return requests, err
		}
		operationContext, cancel := context.WithTimeout(ctx, requestTimeout)
		err := echoRoundTrip(operationContext, connection.conn, payload, connection.sequence, echo, metrics)
		cancel()
		if err != nil {
			return requests, err
		}
		requests++
		connection.sequence++
	}
	return requests, nil
}

func setupRoundTrip(ctx context.Context, conn *websocket.Conn, requestTimeout time.Duration, commandID uint32, request proto.Message, responseCommandID uint32, response proto.Message, sequence uint32) error {
	operationContext, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	return roundTrip(operationContext, conn, commandID, request, responseCommandID, response, sequence)
}

func runBounded(ctx context.Context, count int, concurrency int, job func(context.Context, int) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if count == 0 {
		return nil
	}
	if concurrency <= 0 {
		return errors.New("setup concurrency must be positive")
	}
	if concurrency > count {
		concurrency = count
	}
	childContext, cancel := context.WithCancel(ctx)
	defer cancel()
	semaphore := make(chan struct{}, concurrency)
	var (
		wg       sync.WaitGroup
		once     sync.Once
		errorMu  sync.Mutex
		firstErr error
	)
	for index := 0; index < count; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			select {
			case semaphore <- struct{}{}:
			case <-childContext.Done():
				return
			}
			defer func() { <-semaphore }()
			if err := childContext.Err(); err != nil {
				return
			}
			if err := job(childContext, index); err != nil {
				once.Do(func() {
					errorMu.Lock()
					firstErr = fmt.Errorf("item %d: %w", index, err)
					errorMu.Unlock()
					cancel()
				})
			}
		}(index)
	}
	wg.Wait()
	errorMu.Lock()
	err := firstErr
	errorMu.Unlock()
	if err != nil {
		return err
	}
	return ctx.Err()
}

func closePreparedConnections(prepared []preparedConnection) {
	for _, connection := range prepared {
		closeLoadConnection(connection.conn)
	}
}

func echoRoundTrip(ctx context.Context, conn *websocket.Conn, payload []byte, sequence uint32, echo echoCommands, metrics *loadMetrics) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	stopClose := context.AfterFunc(ctx, func() {
		_ = conn.Close()
	})
	defer stopClose()
	request := gateproduct.EncodeWebSocketPacket(gateproduct.WebSocketPacket{CommandID: echo.requestCommandID, Sequence: sequence, Payload: mustMarshal(&protocol.EchoRequest{Payload: payload})})
	startedAt := time.Now()
	metrics.startEcho()
	defer func() {
		if err != nil {
			_ = conn.Close()
		}
		metrics.finishEcho(ctx, err, time.Since(startedAt))
	}()
	if err = writeMessage(ctx, conn, websocket.BinaryMessage, request); err != nil {
		return err
	}
	_, err = readResponse(ctx, conn, echo.responseCommandID)
	return err
}

func closeLoadConnection(conn *websocket.Conn) {
	if conn == nil {
		return
	}
	deadline := time.Now().Add(time.Second)
	_ = conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		deadline,
	)
	_ = conn.Close()
}

func roundTrip(ctx context.Context, conn *websocket.Conn, commandID uint32, request proto.Message, responseCommandID uint32, response proto.Message, sequence uint32) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	stopClose := context.AfterFunc(ctx, func() {
		_ = conn.Close()
	})
	defer stopClose()
	defer func() {
		if err != nil {
			_ = conn.Close()
		}
	}()
	payload, err := proto.Marshal(request)
	if err != nil {
		return err
	}
	if err := writeMessage(ctx, conn, websocket.BinaryMessage, gateproduct.EncodeWebSocketPacket(gateproduct.WebSocketPacket{CommandID: commandID, Sequence: sequence, Payload: payload})); err != nil {
		return err
	}
	data, err := readResponse(ctx, conn, responseCommandID)
	if err != nil {
		return err
	}
	if response == nil {
		return nil
	}
	err = proto.Unmarshal(data, response)
	return err
}

func readResponse(ctx context.Context, conn *websocket.Conn, commandID uint32) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var deadline time.Time
	if value, ok := ctx.Deadline(); ok {
		deadline = value
	}
	if err := conn.SetReadDeadline(deadline); err != nil {
		return nil, err
	}
	for {
		messageType, data, err := conn.ReadMessage()
		if err != nil {
			return nil, err
		}
		if messageType != websocket.BinaryMessage || len(data) < 16 {
			continue
		}
		if binary.BigEndian.Uint32(data[0:4]) != commandID {
			continue
		}
		size := int(binary.BigEndian.Uint32(data[4:8]))
		if size < 16 || size > len(data) {
			return nil, fmt.Errorf("invalid response packet size %d", size)
		}
		return data[16:size], nil
	}
}

func writeMessage(ctx context.Context, conn *websocket.Conn, messageType int, data []byte) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var deadline time.Time
	if value, ok := ctx.Deadline(); ok {
		deadline = value
	}
	if err := conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	return conn.WriteMessage(messageType, data)
}

func mustMarshal(message proto.Message) []byte {
	payload, err := proto.Marshal(message)
	if err != nil {
		panic(err)
	}
	return payload
}
