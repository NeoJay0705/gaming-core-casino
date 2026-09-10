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
)
