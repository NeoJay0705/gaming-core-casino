// Package gateproto defines the player-facing Gate binary message contracts.
package gateproto

const (
	// LoginRequestCommandID is the client-to-Gate login command.
	LoginRequestCommandID uint32 = 0xC00002
	// LoginResponseCommandID is the Gate-to-client login result command.
	LoginResponseCommandID uint32 = 0xC00003
)
