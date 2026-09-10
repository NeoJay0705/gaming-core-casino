package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/examples/metrics/internal/protocol"
	"github.com/NeoJay0705/gaming-core-casino/pkg/gateproto"
	"github.com/NeoJay0705/gaming-core-casino/products/gateproduct"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

// 保留 protobuf 與既有 WebSocket header 的空間，讓 request/response 都不
// 超過共用的 1 MiB application packet 上限。
const maxPayloadBytes = 1024*1024 - 32

func main() {
	gateURL := flag.String("gate-url", "ws://127.0.0.1:18080/ws", "Gate WebSocket URL")
	connections := flag.Int("connections", 1, "number of closed-loop WebSocket connections")
	duration := flag.Duration("duration", 30*time.Second, "load duration")
	payloadBytes := flag.Int("payload-bytes", 32, "Echo payload size, bounded to 1 MiB")
	flag.Parse()
	if *connections <= 0 || *duration <= 0 || *payloadBytes < 0 || *payloadBytes > maxPayloadBytes {
		log.Fatalf("connections and duration must be positive; payload-bytes must be between 0 and %d", maxPayloadBytes)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()
	startedAt := time.Now()
	payload := make([]byte, *payloadBytes)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}

	var (
		wg           sync.WaitGroup
		successes    atomic.Uint64
		failures     atomic.Uint64
		echoRequests atomic.Uint64
	)
	runID := startedAt.UnixNano()
	for i := 0; i < *connections; i++ {
		loginName := fmt.Sprintf("load-%d-%d", runID, i)
		wg.Add(1)
		go func(loginName string) {
			defer wg.Done()
			requests, err := runConnection(ctx, *gateURL, payload, loginName)
			echoRequests.Add(requests)
			if err != nil && !isExpectedLoadTermination(ctx, err) {
				failures.Add(1)
				log.Printf("load connection failed: %v", err)
				return
			}
			successes.Add(1)
		}(loginName)
	}
	wg.Wait()
	log.Printf("load complete: connections=%d success=%d failures=%d echo_requests=%d duration=%s", *connections, successes.Load(), failures.Load(), echoRequests.Load(), time.Since(startedAt).Round(time.Millisecond))
}

// isExpectedLoadTermination 將全域 duration 到期時的 socket timeout 視為正常收尾。
// deadline timer 與 context cancellation 可能有極小的排程差，因此不能只檢查 ctx.Err。
func isExpectedLoadTermination(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Now().Before(deadline) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var timeoutErr net.Error
	return errors.As(err, &timeoutErr) && timeoutErr.Timeout()
}

func runConnection(ctx context.Context, gateURL string, payload []byte, loginName string) (uint64, error) {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, gateURL, nil)
	if err != nil {
		return 0, err
	}
	defer closeLoadConnection(conn)
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetWriteDeadline(deadline); err != nil {
			_ = conn.Close()
			return 0, fmt.Errorf("set write deadline: %w", err)
		}
	}
	sequence := uint32(1)
	if err := roundTrip(ctx, conn, gateproto.LoginRequestCommandID, &gateproto.LoginRequest{LoginName: loginName}, gateproto.LoginResponseCommandID, &gateproto.LoginResponse{}, sequence); err != nil {
		return 0, fmt.Errorf("login: %w", err)
	}
	sequence++
	if err := roundTrip(ctx, conn, protocol.EnterRoomRequestCommandID, &protocol.EnterRoomRequest{RoomId: "load-room"}, protocol.EnterRoomResponseCommandID, &protocol.EnterRoomResponse{}, sequence); err != nil {
		return 0, fmt.Errorf("enter room: %w", err)
	}
	sequence++
	var requests uint64
	for ctx.Err() == nil {
		request := gateproduct.EncodeWebSocketPacket(gateproduct.WebSocketPacket{CommandID: protocol.EchoRequestCommandID, Sequence: sequence, Payload: mustMarshal(&protocol.EchoRequest{Payload: payload})})
		if err := conn.WriteMessage(websocket.BinaryMessage, request); err != nil {
			return requests, err
		}
		if _, err := readResponse(ctx, conn, protocol.EchoResponseCommandID); err != nil {
			return requests, err
		}
		requests++
		sequence++
	}
	return requests, ctx.Err()
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

func roundTrip(ctx context.Context, conn *websocket.Conn, commandID uint32, request proto.Message, responseCommandID uint32, response proto.Message, sequence uint32) error {
	payload, err := proto.Marshal(request)
	if err != nil {
		return err
	}
	if err := conn.WriteMessage(websocket.BinaryMessage, gateproduct.EncodeWebSocketPacket(gateproduct.WebSocketPacket{CommandID: commandID, Sequence: sequence, Payload: payload})); err != nil {
		return err
	}
	data, err := readResponse(ctx, conn, responseCommandID)
	if err != nil {
		return err
	}
	if response == nil {
		return nil
	}
	return proto.Unmarshal(data, response)
}

func readResponse(ctx context.Context, conn *websocket.Conn, commandID uint32) ([]byte, error) {
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetReadDeadline(deadline); err != nil {
			return nil, err
		}
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

func mustMarshal(message proto.Message) []byte {
	payload, err := proto.Marshal(message)
	if err != nil {
		panic(err)
	}
	return payload
}
