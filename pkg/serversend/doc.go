// Package serversend defines transport-neutral server-originated client
// delivery contracts. It separates request-bound, routed-player, and
// broadcast delivery so a private message can never silently become a
// broadcast when its route is unavailable.
package serversend
