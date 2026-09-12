package gatelink

import (
	"encoding/binary"
	"hash/fnv"
	"sync/atomic"

	"google.golang.org/grpc/connectivity"
)

// endpointSnapshot 發布後不可修改。DNS loop 準備新 snapshot 時，Forward 仍可
// 不取得 topology lock 直接讀取目前版本。
type endpointSnapshot struct {
	endpoints []*endpointPool
}

// endpointPool 擁有一個 resolved Game endpoint 的獨立 HTTP/2 connections。
// endpoint address 是 affinity identity；connection 選擇只在同一 Game process
// 內分散 streams。
type endpointPool struct {
	address string
	conns   []*clientConnection
	next    atomic.Uint64
}

type clientConnection struct {
	conn   clientConn
	client GateRequestServiceClient
}

// clientConn 是 picker 使用的 grpc.ClientConn 最小介面，讓 routing unit test
// 不必建立實際網路連線。
type clientConn interface {
	Connect()
	GetState() connectivity.State
	Close() error
}

// hasReady 只觀察 endpoint 的 connection state，不推進 round-robin cursor。
// gRPC channel 進入 Idle 時，重新呼叫 Connect 讓後續 request 有機會恢復此
// transport；不等待非同步連線完成，當前 request 仍遵守 ready-aware picker。
func (p *endpointPool) hasReady() bool {
	if p == nil {
		return false
	}
	ready := false
	for _, connection := range p.conns {
		if connection == nil || connection.conn == nil {
			continue
		}
		switch connection.conn.GetState() {
		case connectivity.Ready:
			ready = true
		case connectivity.Idle:
			connection.conn.Connect()
		}
	}
	return ready
}

// pick 以 round-robin 選擇 ready connection。若沒有 ready connection，回傳
// deterministic fallback 並標記 ready=false，交由 caller 保留 grpc-go 原本的
// connectivity／error 行為。
func (p *endpointPool) pick() (*clientConnection, bool) {
	if p == nil || len(p.conns) == 0 {
		return nil, false
	}
	start := p.next.Add(1) - 1
	var fallback *clientConnection
	for offset := 0; offset < len(p.conns); offset++ {
		connection := p.conns[(start+uint64(offset))%uint64(len(p.conns))]
		if fallback == nil {
			fallback = connection
		}
		if connection != nil && connection.conn != nil && connection.conn.GetState() == connectivity.Ready {
			return connection, true
		}
	}
	return fallback, false
}

// pickEndpoint 對所有 endpoint identity 套用 rendezvous hashing，同時優先選擇
// 目前有 ready connection 且分數最高的 endpoint。沒有 ready connection 時仍
// 保留 preferred endpoint，讓 grpc-go 回報原本的 connectivity error。
func pickEndpoint(snapshot *endpointSnapshot, affinityKey string) (*endpointPool, *clientConnection) {
	if snapshot == nil || len(snapshot.endpoints) == 0 || affinityKey == "" {
		return nil, nil
	}
	var preferred *endpointPool
	var preferredScore uint64
	preferredSet := false
	var readyPool *endpointPool
	var readyScore uint64
	readySet := false
	for _, endpoint := range snapshot.endpoints {
		if endpoint == nil || len(endpoint.conns) == 0 {
			continue
		}
		score := rendezvousScore(affinityKey, endpoint.address)
		if !preferredSet || score > preferredScore {
			preferred, preferredScore, preferredSet = endpoint, score, true
		}
		if endpoint.hasReady() && (!readySet || score > readyScore) {
			readyPool, readyScore, readySet = endpoint, score, true
		}
	}
	selected := preferred
	if readySet {
		selected = readyPool
	}
	if !preferredSet {
		return nil, nil
	}
	connection, _ := selected.pick()
	return selected, connection
}

// rendezvousScore 使用固定的 FNV-1a hash 與不歧義的 key/address framing。
// process-random hash seed 會破壞跨 Gate affinity。
func rendezvousScore(affinityKey, address string) uint64 {
	hasher := fnv.New64a()
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(affinityKey)))
	_, _ = hasher.Write(length[:])
	_, _ = hasher.Write([]byte(affinityKey))
	binary.BigEndian.PutUint64(length[:], uint64(len(address)))
	_, _ = hasher.Write(length[:])
	_, _ = hasher.Write([]byte(address))
	return hasher.Sum64()
}
