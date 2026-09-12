package gatelink

import (
	"context"
	"errors"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
)

func TestContractRendezvousAffinityIsStableAndDistributed(t *testing.T) {
	snapshot := testPickerSnapshot("game-a:9090", "game-b:9090", "game-c:9090")
	counts := make(map[string]int)
	for index := 0; index < 300; index++ {
		key := "player-" + string(rune('a'+index%26)) + "-" + string(rune('0'+index%10))
		first, _ := pickEndpoint(snapshot, key)
		second, _ := pickEndpoint(snapshot, key)
		if first == nil || second == nil || first.address != second.address {
			t.Fatalf("key %q selected %v then %v", key, first, second)
		}
		counts[first.address]++
	}
	for _, address := range []string{"game-a:9090", "game-b:9090", "game-c:9090"} {
		if counts[address] == 0 {
			t.Fatalf("endpoint %q received no affinity keys: %#v", address, counts)
		}
	}
}

func TestContractRendezvousTopologyChangesMoveOnlyNecessaryKeys(t *testing.T) {
	base := testPickerSnapshot("game-a:9090", "game-b:9090")
	added := testPickerSnapshot("game-a:9090", "game-b:9090", "game-c:9090")
	for index := 0; index < 500; index++ {
		key := "player-" + string(rune(index))
		before, _ := pickEndpoint(base, key)
		after, _ := pickEndpoint(added, key)
		if before == nil || after == nil {
			t.Fatalf("key %q produced nil endpoint", key)
		}
		if before.address != after.address && after.address != "game-c:9090" {
			t.Fatalf("adding endpoint remapped %q from %q to existing %q", key, before.address, after.address)
		}
	}

	removed := testPickerSnapshot("game-a:9090", "game-b:9090")
	for index := 0; index < 500; index++ {
		key := "player-" + string(rune(index))
		before, _ := pickEndpoint(added, key)
		after, _ := pickEndpoint(removed, key)
		if before == nil || after == nil {
			t.Fatalf("key %q produced nil endpoint", key)
		}
		if before.address != "game-c:9090" && before.address != after.address {
			t.Fatalf("removing endpoint remapped %q from %q to %q", key, before.address, after.address)
		}
	}
}

func TestContractReadyEndpointFallbackPreservesAvailability(t *testing.T) {
	preferred := &endpointPool{address: "game-a:9090", conns: []*clientConnection{{conn: &pickerConn{state: connectivity.TransientFailure}}}}
	ready := &endpointPool{address: "game-b:9090", conns: []*clientConnection{{conn: &pickerConn{state: connectivity.Ready}}}}
	snapshot := &endpointSnapshot{endpoints: []*endpointPool{preferred, ready}}
	for index := 0; index < 100; index++ {
		pool, connection := pickEndpoint(snapshot, "player-"+string(rune(index)))
		if pool != ready || connection == nil {
			t.Fatalf("unready preferred endpoint selected pool=%v connection=%v", pool, connection)
		}
	}
}

func TestContractEndpointConnectionsRoundRobinWithinAffinity(t *testing.T) {
	first := &pickerConn{state: connectivity.Ready}
	second := &pickerConn{state: connectivity.Ready}
	pool := &endpointPool{address: "game-a:9090", conns: []*clientConnection{{conn: first}, {conn: second}}}
	for index, expected := range []*clientConnection{pool.conns[0], pool.conns[1], pool.conns[0], pool.conns[1]} {
		got, ready := pool.pick()
		if !ready || got != expected {
			t.Fatalf("pick %d = %p ready=%t, want %p/true", index, got, ready, expected)
		}
	}
}

func TestContractEndpointScanDoesNotAdvanceUnselectedPool(t *testing.T) {
	first := &endpointPool{address: "game-a:9090", conns: []*clientConnection{
		{conn: &pickerConn{state: connectivity.Ready}},
		{conn: &pickerConn{state: connectivity.Ready}},
	}}
	second := &endpointPool{address: "game-b:9090", conns: []*clientConnection{
		{conn: &pickerConn{state: connectivity.Ready}},
		{conn: &pickerConn{state: connectivity.Ready}},
	}}
	snapshot := &endpointSnapshot{endpoints: []*endpointPool{first, second}}
	want := first
	if rendezvousScore("alice", second.address) > rendezvousScore("alice", first.address) {
		want = second
	}

	got, connection := pickEndpoint(snapshot, "alice")
	if got != want || connection == nil {
		t.Fatalf("pickEndpoint() = pool:%v connection:%v, want pool:%v and connection", got, connection, want)
	}
	if got := want.next.Load(); got != 1 {
		t.Fatalf("selected pool next = %d, want 1", got)
	}
	other := first
	if other == want {
		other = second
	}
	if got := other.next.Load(); got != 0 {
		t.Fatalf("unselected pool next = %d, want 0", got)
	}
}

func TestContractReadinessProbeReconnectsIdleConnections(t *testing.T) {
	ready := &pickerConn{state: connectivity.Ready}
	idle := &pickerConn{state: connectivity.Idle}
	pool := &endpointPool{address: "game-a:9090", conns: []*clientConnection{
		{conn: ready},
		{conn: idle},
	}}

	if !pool.hasReady() {
		t.Fatal("hasReady() = false, want true for the Ready connection")
	}
	if got := idle.connect.Load(); got != 1 {
		t.Fatalf("Idle connection Connect() calls = %d, want 1", got)
	}
	if got := pool.next.Load(); got != 0 {
		t.Fatalf("readiness probe advanced next = %d, want 0", got)
	}
}

func TestContractForwardFailureDoesNotRetryAnotherEndpoint(t *testing.T) {
	var calls atomic.Int32
	failure := status.Error(codes.Unavailable, "Game unavailable")
	snapshot := &endpointSnapshot{endpoints: []*endpointPool{
		{address: "game-a:9090", conns: []*clientConnection{{conn: &pickerConn{state: connectivity.Ready}, client: &forwardErrorClient{calls: &calls, err: failure}}}},
		{address: "game-b:9090", conns: []*clientConnection{{conn: &pickerConn{state: connectivity.Ready}, client: &forwardErrorClient{calls: &calls, err: failure}}}},
	}}
	client := &Client{cfg: ClientConfig{Timeout: time.Second}, started: true}
	client.snapshot.Store(snapshot)
	_, err := client.Forward(WithAffinityKey(context.Background(), "alice"), Request{CommandID: 1})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("Forward error status = %s, want %s", status.Code(err), codes.Unavailable)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("failed Forward calls = %d, want one attempt without retry", got)
	}
}

func TestContractConcurrentForwardRefreshAndStop(t *testing.T) {
	resolver := &fakeHostResolver{
		addresses:     []netip.Addr{netip.MustParseAddr("127.0.0.1")},
		lookupStarted: make(chan struct{}),
		lookupRelease: make(chan struct{}),
	}
	forward := &blockingForwardClient{entered: make(chan struct{}), release: make(chan struct{})}
	connection := &pickerConn{state: connectivity.Ready, closeSignal: make(chan struct{})}
	client := &Client{
		cfg:      ClientConfig{Timeout: time.Second},
		target:   clientTarget{dynamic: true, host: "game.test", port: "1"},
		resolver: resolver,
		started:  true,
	}
	client.snapshot.Store(&endpointSnapshot{endpoints: []*endpointPool{{
		address: "127.0.0.1:1",
		conns:   []*clientConnection{{conn: connection, client: forward}},
	}}})

	forwardDone := make(chan error, 1)
	go func() {
		_, err := client.Forward(WithAffinityKey(context.Background(), "alice"), Request{CommandID: 1})
		forwardDone <- err
	}()
	select {
	case <-forward.entered:
	case <-time.After(time.Second):
		t.Fatal("Forward did not acquire the snapshot")
	}

	refreshDone := make(chan struct{})
	go func() {
		client.refreshDNS(context.Background())
		close(refreshDone)
	}()
	select {
	case <-resolver.lookupStarted:
	case <-time.After(time.Second):
		t.Fatal("refresh did not enter resolver")
	}
	stopDone := make(chan error, 1)
	go func() { stopDone <- client.Stop(context.Background()) }()
	select {
	case <-connection.closeSignal:
	case <-time.After(time.Second):
		t.Fatal("Stop did not close the published connection")
	}
	close(forward.release)
	close(resolver.lookupRelease)

	select {
	case <-refreshDone:
	case <-time.After(time.Second):
		t.Fatal("refresh did not finish")
	}
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop did not finish")
	}
	select {
	case err := <-forwardDone:
		if err != nil {
			t.Fatalf("Forward() error = %v, want fake response", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Forward did not finish")
	}
	if got := forward.calls.Load(); got != 1 {
		t.Fatalf("Forward calls = %d, want 1", got)
	}
	if got := connection.close.Load(); got != 1 {
		t.Fatalf("connection Close() calls = %d, want 1", got)
	}
	if got := client.snapshot.Load(); got != nil {
		t.Fatalf("snapshot after Stop = %#v, want nil", got)
	}
}

func TestContractAffinityContextPreservesOpaqueLoginName(t *testing.T) {
	ctx := WithAffinityKey(context.Background(), " alice ")
	if got, ok := AffinityKeyFromContext(ctx); !ok || got != " alice " {
		t.Fatalf("affinity key = %q, present=%t; want opaque whitespace-preserving value", got, ok)
	}
	if _, ok := AffinityKeyFromContext(WithAffinityKey(context.Background(), "")); ok {
		t.Fatal("empty affinity key reported present")
	}
	if _, ok := AffinityKeyFromContext(nil); ok {
		t.Fatal("nil context reported affinity key")
	}
}

func TestContractForwardRequiresAffinityKey(t *testing.T) {
	server, err := NewServer(ServerConfig{ListenAddr: "127.0.0.1:0"}, RequestHandlerFunc(func(context.Context, Request) error {
		t.Fatal("handler called without affinity key")
		return nil
	}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(func() { _ = server.Stop(context.Background()) })
	client, err := NewClient(ClientConfig{Target: server.Addr()})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { _ = client.Stop(context.Background()) })
	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("start client: %v", err)
	}
	ctx := WithGateRequestContext(context.Background(), GateRequestContext{Source: RequestSource{ConnectionID: "connection-1"}})
	if _, err := client.Forward(ctx, Request{CommandID: 1}); !errors.Is(err, ErrAffinityKeyRequired) {
		t.Fatalf("Forward without affinity key error = %v, want %v", err, ErrAffinityKeyRequired)
	}
}

func TestContractForwardBeforeStartFailsLifecycle(t *testing.T) {
	client, err := NewClient(ClientConfig{Target: "127.0.0.1:1"})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	if _, err := client.Forward(WithAffinityKey(context.Background(), "alice"), Request{CommandID: 1}); !errors.Is(err, ErrClientNotStarted) {
		t.Fatalf("Forward before Start error = %v, want %v", err, ErrClientNotStarted)
	}
}

func TestContractClientLifecycleRejectsDuplicateAndRestart(t *testing.T) {
	client, err := NewClient(ClientConfig{Target: "127.0.0.1:1"})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("start client: %v", err)
	}
	if err := client.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "already started") {
		t.Fatalf("duplicate Start error = %v, want lifecycle error", err)
	}
	if err := client.Stop(context.Background()); err != nil {
		t.Fatalf("stop client: %v", err)
	}
	if err := client.Stop(context.Background()); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
	if err := client.Start(context.Background()); !errors.Is(err, ErrClientStopped) {
		t.Fatalf("Start after Stop error = %v, want %v", err, ErrClientStopped)
	}
	if _, err := client.Forward(WithAffinityKey(context.Background(), "alice"), Request{CommandID: 1}); !errors.Is(err, ErrClientStopped) {
		t.Fatalf("Forward after Stop error = %v, want %v", err, ErrClientStopped)
	}
}

func TestContractClientConfigDefaultsAndValidation(t *testing.T) {
	client, err := NewClient(ClientConfig{Target: "dns:///game.test:9090"})
	if err != nil {
		t.Fatalf("new DNS client: %v", err)
	}
	if client.cfg.Timeout != DefaultTimeout || client.cfg.DNSRefreshInterval != DefaultDNSRefreshInterval || client.cfg.ConnectionsPerHost != DefaultConnectionsPerHost {
		t.Fatalf("defaults = timeout:%s refresh:%s connections:%d", client.cfg.Timeout, client.cfg.DNSRefreshInterval, client.cfg.ConnectionsPerHost)
	}
	staticClient, err := NewClient(ClientConfig{Target: "127.0.0.1:9090"})
	if err != nil {
		t.Fatalf("new static client: %v", err)
	}
	if staticClient.cfg.DNSRefreshInterval != DefaultDNSRefreshInterval {
		t.Fatalf("static refresh default = %s, want %s", staticClient.cfg.DNSRefreshInterval, DefaultDNSRefreshInterval)
	}
	for _, test := range []struct {
		name string
		cfg  ClientConfig
		want string
	}{
		{name: "negative timeout", cfg: ClientConfig{Target: "127.0.0.1:1", Timeout: -time.Second}, want: "timeout"},
		{name: "negative refresh", cfg: ClientConfig{Target: "dns:///game.test:9090", DNSRefreshInterval: -time.Second}, want: "dns_refresh_interval"},
		{name: "negative connections", cfg: ClientConfig{Target: "127.0.0.1:1", ConnectionsPerHost: -1}, want: "connections_per_host"},
		{name: "refresh static", cfg: ClientConfig{Target: "127.0.0.1:1", DNSRefreshInterval: time.Second}, want: "requires a dns:/// target"},
		{name: "unsupported scheme", cfg: ClientConfig{Target: "gatelink:///game"}, want: "unsupported target scheme"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewClient(test.cfg); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("NewClient() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestContractDNSLookupCanonicalizesAndSortsAddresses(t *testing.T) {
	resolver := &fakeHostResolver{addresses: []netip.Addr{
		netip.MustParseAddr("10.0.0.2"),
		netip.MustParseAddr("10.0.0.1"),
		netip.MustParseAddr("10.0.0.2"),
	}}
	client, err := newClientWithResolver(ClientConfig{Target: "dns:///game.test:9090", DNSRefreshInterval: time.Second}, resolver, nil)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	addresses, err := client.lookupDNS(context.Background())
	if err != nil {
		t.Fatalf("lookup DNS: %v", err)
	}
	want := []string{"10.0.0.1:9090", "10.0.0.2:9090"}
	if len(addresses) != len(want) || addresses[0] != want[0] || addresses[1] != want[1] {
		t.Fatalf("addresses = %#v, want %#v", addresses, want)
	}
}

func TestContractDNSRefreshReconcilesAndRetainsLastGoodSnapshot(t *testing.T) {
	resolver := &fakeHostResolver{addresses: []netip.Addr{
		netip.MustParseAddr("127.0.0.1"),
		netip.MustParseAddr("127.0.0.2"),
	}}
	client, err := newClientWithResolver(ClientConfig{Target: "dns:///game.test:1", DNSRefreshInterval: time.Hour, ConnectionsPerHost: 2}, resolver, nil)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("start client: %v", err)
	}
	t.Cleanup(func() { _ = client.Stop(context.Background()) })
	waitForSnapshot(t, client, 2)
	for _, endpoint := range client.snapshot.Load().endpoints {
		if got := len(endpoint.conns); got != 2 {
			t.Fatalf("connections for endpoint %q = %d, want 2", endpoint.address, got)
		}
	}
	initialSnapshot := client.snapshot.Load()
	initialPool := initialSnapshot.endpoints[0]
	client.refreshDNS(context.Background())
	if current := client.snapshot.Load(); current != initialSnapshot {
		t.Fatal("unchanged DNS result rebuilt the endpoint snapshot")
	}
	if current := client.snapshot.Load().endpoints[0]; current != initialPool {
		t.Fatal("unchanged DNS result rebuilt the endpoint pool")
	}
	resolver.set([]netip.Addr{netip.MustParseAddr("127.0.0.2")}, nil)
	client.refreshDNS(context.Background())
	waitForEndpoint(t, client, "127.0.0.2:1")
	resolver.set(nil, errors.New("DNS unavailable"))
	client.refreshDNS(context.Background())
	if snapshot := client.snapshot.Load(); snapshot == nil || len(snapshot.endpoints) != 1 || snapshot.endpoints[0].address != "127.0.0.2:1" {
		t.Fatalf("snapshot after failed refresh = %#v, want last good endpoint", snapshot)
	}
	resolver.set([]netip.Addr{netip.MustParseAddr("127.0.0.3")}, nil)
	client.refreshDNS(context.Background())
	waitForEndpoint(t, client, "127.0.0.3:1")
}

func TestContractDNSInitialFailureKeepsClientAvailableForRetry(t *testing.T) {
	resolver := &fakeHostResolver{err: errors.New("DNS unavailable")}
	client, err := newClientWithResolver(ClientConfig{Target: "dns:///game.test:1", DNSRefreshInterval: 5 * time.Millisecond}, resolver, nil)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { _ = client.Stop(context.Background()) })
	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("start client after DNS failure: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for resolver.calls.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if resolver.calls.Load() < 1 {
		t.Fatal("initial DNS lookup did not run")
	}
	if _, err := client.Forward(WithAffinityKey(context.Background(), "alice"), Request{CommandID: 1}); status.Code(err) != codes.Unavailable {
		t.Fatalf("Forward without resolved endpoints status = %s, want %s", status.Code(err), codes.Unavailable)
	}
}

func TestContractDNSRefreshRunsPeriodically(t *testing.T) {
	resolver := &fakeHostResolver{addresses: []netip.Addr{netip.MustParseAddr("127.0.0.1")}}
	client, err := newClientWithResolver(ClientConfig{Target: "dns:///game.test:1", DNSRefreshInterval: 5 * time.Millisecond}, resolver, nil)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("start client: %v", err)
	}
	t.Cleanup(func() { _ = client.Stop(context.Background()) })
	deadline := time.Now().Add(time.Second)
	for resolver.calls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := resolver.calls.Load(); got < 2 {
		t.Fatalf("DNS lookups = %d, want initial lookup plus periodic refresh", got)
	}
}

func testPickerSnapshot(addresses ...string) *endpointSnapshot {
	endpoints := make([]*endpointPool, 0, len(addresses))
	for _, address := range addresses {
		endpoints = append(endpoints, &endpointPool{address: address, conns: []*clientConnection{{conn: &pickerConn{state: connectivity.Ready}}}})
	}
	return &endpointSnapshot{endpoints: endpoints}
}

type pickerConn struct {
	state       connectivity.State
	close       atomic.Int32
	connect     atomic.Int32
	closeSignal chan struct{}
	closeOnce   sync.Once
}

func (c *pickerConn) Connect()                     { c.connect.Add(1) }
func (c *pickerConn) GetState() connectivity.State { return c.state }
func (c *pickerConn) Close() error {
	c.close.Add(1)
	if c.closeSignal != nil {
		c.closeOnce.Do(func() { close(c.closeSignal) })
	}
	return nil
}

type forwardErrorClient struct {
	calls *atomic.Int32
	err   error
}

func (c *forwardErrorClient) Forward(context.Context, *GateRequest, ...grpc.CallOption) (*ForwardResponse, error) {
	c.calls.Add(1)
	return nil, c.err
}

type blockingForwardClient struct {
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (c *blockingForwardClient) Forward(context.Context, *GateRequest, ...grpc.CallOption) (*ForwardResponse, error) {
	c.calls.Add(1)
	close(c.entered)
	<-c.release
	return &ForwardResponse{}, nil
}

type fakeHostResolver struct {
	mu            sync.Mutex
	addresses     []netip.Addr
	err           error
	calls         atomic.Int32
	lookupStarted chan struct{}
	lookupRelease chan struct{}
	lookupOnce    sync.Once
}

func (r *fakeHostResolver) LookupNetIP(_ context.Context, _, _ string) ([]netip.Addr, error) {
	r.calls.Add(1)
	if r.lookupStarted != nil {
		r.lookupOnce.Do(func() { close(r.lookupStarted) })
		<-r.lookupRelease
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]netip.Addr(nil), r.addresses...), r.err
}

func (r *fakeHostResolver) set(addresses []netip.Addr, err error) {
	r.mu.Lock()
	r.addresses, r.err = append([]netip.Addr(nil), addresses...), err
	r.mu.Unlock()
}

func waitForSnapshot(t *testing.T, client *Client, count int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if snapshot := client.snapshot.Load(); snapshot != nil && len(snapshot.endpoints) == count {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("snapshot did not reach %d endpoints", count)
}

func waitForEndpoint(t *testing.T, client *Client, address string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if snapshot := client.snapshot.Load(); snapshot != nil && len(snapshot.endpoints) == 1 && snapshot.endpoints[0].address == address {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("snapshot did not reach endpoint %q", address)
}
