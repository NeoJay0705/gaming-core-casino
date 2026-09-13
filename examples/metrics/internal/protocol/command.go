package protocol

const (
	// EnterRoomRequestCommandID 是範例 client-to-Gate 進房 command。
	EnterRoomRequestCommandID uint32 = 0xF1000001
	// EnterRoomResponseCommandID 是範例 Gate-to-client 進房結果。
	EnterRoomResponseCommandID uint32 = 0xF1000002
	// EchoRequestCommandID 是範例 forward 至 Game 的 command。
	EchoRequestCommandID uint32 = 0xF1000011
	// EchoResponseCommandID 是範例 Game-to-client 結果。
	EchoResponseCommandID uint32 = 0xF1000012
	// LocalEchoRequestCommandID 是 Gate-local 對照路徑的 request；payload
	// 沿用 EchoRequest，只用於與 Game Echo 隔離瓶頸。
	LocalEchoRequestCommandID uint32 = 0xF1000021
	// LocalEchoResponseCommandID 是 Gate-local 對照路徑的 response。
	LocalEchoResponseCommandID uint32 = 0xF1000022
	// BroadcastRoomCommandID 是範例 remote-command dispatcher 的 routing ID。
	// room_id 與 client payload 位於 BroadcastRoomCommand protobuf，不屬於
	// framework transport contract。
	BroadcastRoomCommandID uint32 = 0xF1000031
	// StartPushRequestCommandID 是範例壓測控制 command；由第一條 client
	// connection 在所有 session ready 後送出。
	StartPushRequestCommandID uint32 = 0xF1000041
	// StartPushResponseCommandID 是 Game 接受壓測 workload 後回 controller
	// connection 的 response。
	StartPushResponseCommandID uint32 = 0xF1000042
	// PushMessageCommandID 是 Broadcast／Player 最後送往 WebSocket client
	// 的共同 command。
	PushMessageCommandID uint32 = 0xF1000043
)
