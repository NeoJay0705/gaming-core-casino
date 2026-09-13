package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/examples/metrics/internal/protocol"
	"github.com/NeoJay0705/gaming-core-casino/products/gateproduct"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

const (
	pushWorkloadBroadcast = "broadcast"
	pushWorkloadPlayer    = "player"

	pushFrameHeaderBytes = 16
	pushFrameMaxBytes    = 1024 * 1024
)

var (
	errPushLoadDrain = errors.New("push load: drain timeout")
)

func parsePushWorkload(value string) (string, error) {
	switch value = strings.ToLower(strings.TrimSpace(value)); value {
	case pushWorkloadBroadcast, pushWorkloadPlayer:
		return value, nil
	default:
		return "", fmt.Errorf("workload must be %q, %q, or %q", "echo", pushWorkloadBroadcast, pushWorkloadPlayer)
	}
}

type pushReaderStats struct {
	received     uint64
	duplicate    uint64
	sequenceGap  uint64
	missing      uint64
	invalid      uint64
	lastSequence uint64
}

type pushLoadStats struct {
	received       uint64
	duplicate      uint64
	sequenceGap    uint64
	invalid        uint64
	readerFailures uint64
}

// pushClient 為每條 WebSocket 維護唯一的 reader goroutine。writer 仍由呼叫端
// 執行，避免並行呼叫 ReadMessage，同時允許非同步 server-send frame 到達。
type pushClient struct {
	ctx    context.Context
	cancel context.CancelFunc

	readers     []*pushReader
	metrics     *loadMetrics
	mode        string
	payloadSize int

	controlResponses chan *protocol.StartPushResponse
	readerErrors     chan error
	notify           chan struct{}

	mu             sync.Mutex
	started        bool
	stopping       bool
	readerFailures uint64
	stopOnce       sync.Once
	stopDone       chan struct{}
	readerWG       sync.WaitGroup
}

type pushReader struct {
	client *pushClient
	conn   *websocket.Conn
	index  int

	mu       sync.Mutex
	runID    string
	last     uint64
	stats    pushReaderStats
	measured bool
}

func newPushClient(ctx context.Context, prepared []preparedConnection, metrics *loadMetrics, mode string, payloadSize int) (*pushClient, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(prepared) == 0 {
		return nil, errors.New("push load: at least one prepared connection is required")
	}
	if metrics == nil {
		return nil, errors.New("push load: metrics are required")
	}
	if mode != pushWorkloadBroadcast && mode != pushWorkloadPlayer {
		return nil, fmt.Errorf("push load: unsupported mode %q", mode)
	}
	if payloadSize < 0 || payloadSize > maxPayloadBytes {
		return nil, fmt.Errorf("push load: payload bytes must be between 0 and %d", maxPayloadBytes)
	}
	readerContext, cancel := context.WithCancel(ctx)
	client := &pushClient{
		ctx:              readerContext,
		cancel:           cancel,
		metrics:          metrics,
		mode:             mode,
		payloadSize:      payloadSize,
		controlResponses: make(chan *protocol.StartPushResponse, 1),
		readerErrors:     make(chan error, 1),
		notify:           make(chan struct{}, 1),
		stopDone:         make(chan struct{}),
	}
	client.readers = make([]*pushReader, len(prepared))
	for index := range prepared {
		if prepared[index].conn == nil {
			cancel()
			return nil, fmt.Errorf("push load: prepared connection %d is nil", index)
		}
		client.readers[index] = &pushReader{client: client, conn: prepared[index].conn, index: index}
	}
	// cancellation callback 關閉所有 connection，解除等待中的 ReadMessage，
	// 不需週期性 read-deadline syscall。
	context.AfterFunc(readerContext, func() {
		for _, reader := range client.readers {
			_ = reader.conn.Close()
		}
	})
	return client, nil
}

func (c *pushClient) Start() error {
	if c == nil {
		return errors.New("push load: client is nil")
	}
	c.mu.Lock()
	if c.stopping {
		c.mu.Unlock()
		return errors.New("push load: client is stopped")
	}
	if c.started {
		c.mu.Unlock()
		return errors.New("push load: readers are already started")
	}
	c.started = true
	c.mu.Unlock()
	for _, reader := range c.readers {
		// setup 的 direct read 會設定 request timeout；非同步 push reader
		// 必須清除該 deadline，生命週期由 cancellation 關閉 connection。
		if err := reader.conn.SetReadDeadline(time.Time{}); err != nil {
			c.cancel()
			return fmt.Errorf("push load: clear reader %d read deadline: %w", reader.index, err)
		}
		c.metrics.startPushReader()
		c.readerWG.Add(1)
		go func(reader *pushReader) {
			defer c.readerWG.Done()
			defer c.metrics.finishPushReader()
			reader.loop()
		}(reader)
	}
	return nil
}

func (c *pushClient) Stop() {
	if c == nil {
		return
	}
	c.stopOnce.Do(func() {
		c.mu.Lock()
		c.stopping = true
		c.mu.Unlock()
		c.cancel()
		for _, reader := range c.readers {
			_ = reader.conn.Close()
		}
		c.readerWG.Wait()
		close(c.stopDone)
	})
	<-c.stopDone
}

func (c *pushClient) setRun(runID string, measured bool) {
	for _, reader := range c.readers {
		reader.mu.Lock()
		reader.runID = runID
		reader.last = 0
		reader.stats = pushReaderStats{}
		reader.measured = measured
		reader.mu.Unlock()
	}
}

func (c *pushClient) startRun(ctx context.Context, controller *preparedConnection, request *protocol.StartPushRequest, measured bool, timeout time.Duration) (uint64, error) {
	if c == nil || controller == nil || controller.conn == nil {
		return 0, errors.New("push load: controller connection is required")
	}
	if request == nil {
		return 0, errors.New("push load: start request is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if timeout <= 0 {
		return 0, errors.New("push load: control timeout must be positive")
	}
	if err := c.ensureStarted(); err != nil {
		return 0, err
	}
	c.setRun(request.GetRunId(), measured)
	payload, err := proto.Marshal(request)
	if err != nil {
		return 0, fmt.Errorf("push load: encode start request: %w", err)
	}
	sequence := controller.sequence
	controller.sequence++
	controlContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	packet := gateproduct.EncodeWebSocketPacket(gateproduct.WebSocketPacket{
		CommandID: protocol.StartPushRequestCommandID,
		Sequence:  sequence,
		Payload:   payload,
	})
	if err := writeMessage(controlContext, controller.conn, websocket.BinaryMessage, packet); err != nil {
		return 0, fmt.Errorf("push load: write start request: %w", err)
	}
	select {
	case response := <-c.controlResponses:
		if response == nil {
			return 0, errors.New("push load: control response is nil")
		}
		if response.GetRunId() != request.GetRunId() {
			return 0, fmt.Errorf("push load: control response run_id %q does not match %q", response.GetRunId(), request.GetRunId())
		}
		if response.GetPlannedTicks() == 0 {
			return 0, errors.New("push load: control response planned_ticks is zero")
		}
		return response.GetPlannedTicks(), nil
	case err := <-c.readerErrors:
		return 0, fmt.Errorf("push load: reader failed while waiting for control response: %w", err)
	case <-controlContext.Done():
		return 0, fmt.Errorf("push load: wait for control response: %w", controlContext.Err())
	}
}

func (c *pushClient) ensureStarted() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.started || c.stopping {
		return errors.New("push load: readers are not running")
	}
	return nil
}

func (c *pushClient) waitForWarmup(ctx context.Context, planned uint64, timeout time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if planned == 0 {
		return errors.New("push load: warm-up planned ticks must be positive")
	}
	if timeout <= 0 {
		return errors.New("push load: warm-up timeout must be positive")
	}
	waitContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		stats := c.snapshot()
		if stats.duplicate > 0 || stats.sequenceGap > 0 || stats.invalid > 0 {
			return fmt.Errorf("push load: warm-up delivery validation failed: received=%d duplicate=%d sequence_gap=%d invalid=%d", stats.received, stats.duplicate, stats.sequenceGap, stats.invalid)
		}
		if stats.received == planned*uint64(len(c.readers)) && stats.duplicate == 0 && stats.sequenceGap == 0 && stats.invalid == 0 {
			return nil
		}
		select {
		case <-c.notify:
		case err := <-c.readerErrors:
			return fmt.Errorf("push load: warm-up reader failed: %w", err)
		case <-waitContext.Done():
			if errors.Is(waitContext.Err(), context.DeadlineExceeded) {
				return errPushLoadDrain
			}
			return waitContext.Err()
		}
	}
}

func (c *pushClient) waitForQuiet(ctx context.Context, quiet time.Duration) error {
	if quiet <= 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	timer := time.NewTimer(quiet)
	defer timer.Stop()
	for {
		select {
		case <-c.notify:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(quiet)
		case err := <-c.readerErrors:
			return fmt.Errorf("push load: quiet drain reader failed: %w", err)
		case <-timer.C:
			if stats := c.snapshot(); stats.readerFailures > 0 {
				return fmt.Errorf("push load: quiet drain reader failure count=%d", stats.readerFailures)
			}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *pushClient) waitMeasured(ctx context.Context, duration, drain time.Duration) error {
	if duration <= 0 || drain <= 0 {
		return errors.New("push load: measured duration and drain timeout must be positive")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		if stats := c.snapshot(); stats.readerFailures > 0 {
			return fmt.Errorf("push load: measured reader failure count=%d", stats.readerFailures)
		}
	case err := <-c.readerErrors:
		return fmt.Errorf("push load: measured reader failed: %w", err)
	case <-ctx.Done():
		return ctx.Err()
	}

	drainTimer := time.NewTimer(drain)
	defer drainTimer.Stop()
	select {
	case <-drainTimer.C:
		if stats := c.snapshot(); stats.readerFailures > 0 {
			return fmt.Errorf("push load: drain reader failure count=%d", stats.readerFailures)
		}
		return nil
	case err := <-c.readerErrors:
		return fmt.Errorf("push load: drain reader failed: %w", err)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *pushClient) snapshot() pushLoadStats {
	c.mu.Lock()
	stats := pushLoadStats{readerFailures: c.readerFailures}
	c.mu.Unlock()
	for _, reader := range c.readers {
		reader.mu.Lock()
		stats.received += reader.stats.received
		stats.duplicate += reader.stats.duplicate
		stats.sequenceGap += reader.stats.sequenceGap
		stats.invalid += reader.stats.invalid
		reader.mu.Unlock()
	}
	return stats
}

func (c *pushClient) missingAtLeast(planned uint64) uint64 {
	var missing uint64
	for _, reader := range c.readers {
		reader.mu.Lock()
		missing += reader.stats.missing
		if reader.stats.lastSequence < planned {
			missing += planned - reader.stats.lastSequence
		}
		reader.mu.Unlock()
	}
	return missing
}

func (r *pushReader) loop() {
	for {
		messageType, data, err := r.conn.ReadMessage()
		if err != nil {
			if r.client.ctx.Err() != nil {
				return
			}
			r.report(err)
			return
		}
		if messageType != websocket.BinaryMessage {
			r.record(pushResultInvalid, 0)
			r.report(errors.New("non-binary WebSocket frame"))
			return
		}
		packet, err := decodeLoadPacket(data)
		if err != nil {
			r.record(pushResultInvalid, 0)
			r.report(err)
			return
		}
		switch packet.CommandID {
		case protocol.StartPushResponseCommandID:
			response := new(protocol.StartPushResponse)
			if err := proto.Unmarshal(packet.Payload, response); err != nil {
				r.record(pushResultInvalid, 0)
				r.report(fmt.Errorf("decode start response: %w", err))
				return
			}
			select {
			case r.client.controlResponses <- response:
			default:
				r.report(errors.New("multiple start responses pending"))
				return
			}
		case protocol.PushMessageCommandID:
			r.handlePush(packet.Payload)
		default:
			r.record(pushResultInvalid, 0)
			r.report(fmt.Errorf("unexpected push command %#x", packet.CommandID))
			return
		}
	}
}

func (r *pushReader) report(err error) {
	if err == nil || r.client.ctx.Err() != nil {
		return
	}
	r.client.mu.Lock()
	r.client.readerFailures++
	r.client.mu.Unlock()
	select {
	case r.client.readerErrors <- fmt.Errorf("connection %d: %w", r.index, err):
	default:
	}
	r.client.notifyChange()
}

func (r *pushReader) handlePush(payload []byte) {
	message := new(protocol.PushMessage)
	if err := proto.Unmarshal(payload, message); err != nil {
		r.record(pushResultInvalid, 0)
		return
	}
	elapsed := time.Since(time.Unix(0, message.GetSentUnixNano()))
	r.mu.Lock()
	runID := r.runID
	payloadSize := r.client.payloadSize
	measured := r.measured
	last := r.last
	if message.GetRunId() == "" || message.GetRunId() != runID || message.GetSequence() == 0 || len(message.GetPayload()) != payloadSize || message.GetSentUnixNano() <= 0 || elapsed < 0 {
		r.mu.Unlock()
		r.record(pushResultInvalid, 0)
		return
	}
	sequence := message.GetSequence()
	result := pushResultReceived
	missing := uint64(0)
	switch {
	case last == 0 && sequence != 1:
		result = pushResultSequenceGap
		missing = sequence - 1
	case last > 0 && sequence <= last:
		result = pushResultDuplicate
	case last > 0 && sequence != last+1:
		result = pushResultSequenceGap
		missing = sequence - last - 1
	}
	if result != pushResultDuplicate {
		r.last = sequence
	}
	if result == pushResultReceived {
		r.stats.received++
	} else if result == pushResultDuplicate {
		r.stats.duplicate++
	} else {
		r.stats.sequenceGap++
		r.stats.missing += missing
	}
	r.stats.lastSequence = r.last
	r.mu.Unlock()

	if measured {
		r.client.metrics.observePush(r.client.mode, result, elapsed)
	}
	r.client.notifyChange()
}

func (r *pushReader) record(result string, elapsed time.Duration) {
	r.mu.Lock()
	if result == pushResultInvalid {
		r.stats.invalid++
	}
	measured := r.measured
	r.mu.Unlock()
	if measured {
		r.client.metrics.observePush(r.client.mode, result, elapsed)
	}
	r.client.notifyChange()
}

func (c *pushClient) notifyChange() {
	select {
	case c.notify <- struct{}{}:
	default:
	}
}

func decodeLoadPacket(data []byte) (gateproduct.WebSocketPacket, error) {
	if len(data) < pushFrameHeaderBytes {
		return gateproduct.WebSocketPacket{}, fmt.Errorf("packet shorter than %d-byte header", pushFrameHeaderBytes)
	}
	size := int(binary.BigEndian.Uint32(data[4:8]))
	if size < pushFrameHeaderBytes || size > pushFrameMaxBytes || size != len(data) {
		return gateproduct.WebSocketPacket{}, fmt.Errorf("invalid packet size %d for frame length %d", size, len(data))
	}
	return gateproduct.WebSocketPacket{
		CommandID: binary.BigEndian.Uint32(data[0:4]),
		Sequence:  binary.BigEndian.Uint32(data[8:12]),
		Session:   binary.BigEndian.Uint16(data[12:14]),
		Version:   binary.BigEndian.Uint16(data[14:16]),
		Payload:   append([]byte(nil), data[16:size]...),
	}, nil
}

// runPushLoad 執行 example-only push validation。orchestration 模式由外部
// script 以 completion marker 協調 warm-up；missed tick 不會阻止 measured。
func runPushLoad(ctx context.Context, gateURL string, workload string, connectionCount int, duration time.Duration, payloadBytes int, metricsAddr string, setupTimeout time.Duration, setupConcurrency int, pushWarmupDuration time.Duration, drainTimeout time.Duration, interval time.Duration, requestTimeout time.Duration, orchestrationDir string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	mode, err := parsePushWorkload(workload)
	if err != nil {
		return err
	}
	if connectionCount <= 0 || duration <= 0 || payloadBytes < 0 || payloadBytes > maxPayloadBytes || setupTimeout <= 0 || setupConcurrency <= 0 || pushWarmupDuration <= 0 || drainTimeout <= 0 || requestTimeout <= 0 {
		return errors.New("invalid push load configuration")
	}
	if interval != 16*time.Millisecond && interval != 33*time.Millisecond {
		return errors.New("push interval must be 16ms or 33ms")
	}
	observer, err := newLoadObserver(metricsAddr)
	if err != nil {
		return fmt.Errorf("start load metrics observer: %w", err)
	}
	prepared := make([]preparedConnection, 0, connectionCount)
	defer func() {
		closePreparedConnections(prepared)
		shutdownContext, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
		if err := observer.Shutdown(shutdownContext); err != nil {
			// 與 Echo mode 一致：benchmark 結果寫入 log 後，metrics shutdown
			// 只做 best-effort。
			log.Printf("stop load metrics observer: %v", err)
		}
		shutdownCancel()
	}()

	setupStartedAt := time.Now()
	setupContext, setupCancel := context.WithTimeout(ctx, setupTimeout)
	defer setupCancel()
	prepared, err = prepareConnections(setupContext, gateURL, connectionCount, setupConcurrency)
	if err != nil {
		return fmt.Errorf("prepare push connections: %w", err)
	}
	if err := initializeConnections(setupContext, prepared, setupConcurrency, requestTimeout); err != nil {
		return fmt.Errorf("initialize push connections: %w", err)
	}
	setupDuration := time.Since(setupStartedAt)
	client, err := newPushClient(ctx, prepared, observer.metrics, mode, payloadBytes)
	if err != nil {
		return err
	}
	// 即使 Start 在中途清除 read deadline 失敗，也要回收已啟動的
	// reader；defer 會覆蓋所有 early-return lifecycle 分支。
	defer client.Stop()
	if err := client.Start(); err != nil {
		return err
	}
	orchestration, err := newPushOrchestration(orchestrationDir)
	if err != nil {
		return err
	}
	if orchestration != nil {
		if err := orchestration.writeClientsReady(len(prepared)); err != nil {
			return fmt.Errorf("write push clients-ready marker: %w", err)
		}
		if err := orchestration.waitForStart(setupContext); err != nil {
			return fmt.Errorf("wait for push start marker: %w", err)
		}
	}

	loginNames := make([]string, len(prepared))
	for index := range prepared {
		loginNames[index] = prepared[index].name
	}
	warmupID := fmt.Sprintf("%d-warmup", time.Now().UnixNano())
	warmupRequest := newPushStartRequest(warmupID, mode, loginNames, interval, pushWarmupDuration, payloadBytes)
	warmupPlanned, err := client.startRun(ctx, &prepared[0], warmupRequest, false, requestTimeout)
	if err != nil {
		return err
	}
	if orchestration != nil {
		if err := orchestration.writeRunStarted(pushWarmupStartedMarker, warmupID); err != nil {
			return fmt.Errorf("write push warm-up marker: %w", err)
		}
		warmupSummary, err := orchestration.waitForWarmupResult(ctx, pushWarmupDuration+drainTimeout, warmupID, warmupPlanned)
		if err != nil {
			return fmt.Errorf("wait for push warm-up result: %w", err)
		}
		drainContext, drainCancel := context.WithTimeout(ctx, drainTimeout)
		err = client.waitForQuiet(drainContext, interval)
		drainCancel()
		if err != nil {
			return fmt.Errorf("push warm-up drain: %w", err)
		}
		warmupStats := client.snapshot()
		log.Printf("push warm-up complete: mode=%s connections=%d planned_ticks=%d attempted=%d success=%d partial=%d error=%d missed=%d received=%d duplicate=%d sequence_gap=%d invalid=%d missing_per_reader_at_least=%d reader_failures=%d", mode, len(prepared), warmupSummary.planned, warmupSummary.attempted, warmupSummary.success, warmupSummary.partial, warmupSummary.err, warmupSummary.missed, warmupStats.received, warmupStats.duplicate, warmupStats.sequenceGap, warmupStats.invalid, client.missingAtLeast(warmupPlanned), warmupStats.readerFailures)
		if err := orchestration.writeWarmupDrained(warmupID, warmupStats, client.missingAtLeast(warmupPlanned)); err != nil {
			return fmt.Errorf("write push warm-up drained marker: %w", err)
		}
		if err := orchestration.waitForBaseline(ctx, drainTimeout, warmupID); err != nil {
			return fmt.Errorf("wait for push baseline marker: %w", err)
		}
	} else {
		if err := client.waitForWarmup(ctx, warmupPlanned, pushWarmupDuration+drainTimeout); err != nil {
			return fmt.Errorf("push warm-up: %w", err)
		}
		drainContext, drainCancel := context.WithTimeout(ctx, drainTimeout)
		err := client.waitForQuiet(drainContext, interval)
		drainCancel()
		if err != nil {
			return fmt.Errorf("push warm-up drain: %w", err)
		}
	}

	measuredID := fmt.Sprintf("%d-measured", time.Now().UnixNano())
	measuredRequest := newPushStartRequest(measuredID, mode, loginNames, interval, duration, payloadBytes)
	measuredPlanned, err := client.startRun(ctx, &prepared[0], measuredRequest, true, requestTimeout)
	if err != nil {
		return err
	}
	if orchestration != nil {
		if err := orchestration.writeRunStarted("measured-started.json", measuredID); err != nil {
			return fmt.Errorf("write push measured marker: %w", err)
		}
	}
	measurementStart := time.Now()
	err = client.waitMeasured(ctx, duration, drainTimeout)
	measurementEnd := time.Now()
	stats := client.snapshot()
	missing := client.missingAtLeast(measuredPlanned)
	status := "success"
	if err != nil || missing != 0 || stats.duplicate != 0 || stats.sequenceGap != 0 || stats.invalid != 0 || stats.readerFailures != 0 {
		status = "failed"
	}
	log.Printf("push load complete: status=%s mode=%s connections=%d setup_duration=%s interval=%s duration=%s planned_ticks=%d measured_elapsed=%s received=%d duplicate=%d sequence_gap=%d invalid=%d missing_per_reader_at_least=%d reader_failures=%d", status, mode, len(prepared), setupDuration.Round(time.Millisecond), interval, duration, measuredPlanned, measurementEnd.Sub(measurementStart).Round(time.Millisecond), stats.received, stats.duplicate, stats.sequenceGap, stats.invalid, missing, stats.readerFailures)
	if orchestration != nil {
		if markerErr := orchestration.writeFinalReady(measuredID); markerErr != nil {
			return fmt.Errorf("write push final-ready marker: %w", markerErr)
		}
		if markerErr := orchestration.waitForFinalScrape(ctx, drainTimeout, measuredID); markerErr != nil {
			return fmt.Errorf("wait for push final-scrape marker: %w", markerErr)
		}
	}
	// 正式 orchestration 在此之前已完成 final scrape；手動 run 則直接
	// 關閉 reader。defer 仍保留作為 early-return 的 lifecycle safety net。
	client.Stop()
	if err != nil {
		return fmt.Errorf("push measured run: %w", err)
	}
	return nil
}

func newPushStartRequest(runID, mode string, loginNames []string, interval, duration time.Duration, payloadBytes int) *protocol.StartPushRequest {
	request := &protocol.StartPushRequest{
		RunId:          runID,
		IntervalMillis: uint64(interval / time.Millisecond),
		DurationMillis: uint64(duration / time.Millisecond),
		PayloadBytes:   uint32(payloadBytes),
		LoginNames:     append([]string(nil), loginNames...),
	}
	if mode == pushWorkloadBroadcast {
		request.Mode = protocol.PushMode_PUSH_MODE_BROADCAST
		request.RoomId = "load-room"
	} else {
		request.Mode = protocol.PushMode_PUSH_MODE_PLAYER
	}
	return request
}
