package rocketmq

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	primitive4 "github.com/apache/rocketmq-client-go/v2/primitive"
	producer4 "github.com/apache/rocketmq-client-go/v2/producer"
)

func TestConfigContractSupportsRocketMQ4(t *testing.T) {
	client, err := New(testSnapshot(t, "rocketmq:\n  endpoint: namesrv:9876\n"))
	if err != nil {
		t.Fatalf("new default RocketMQ client: %v", err)
	}
	if got := client.Config().Endpoint; got != "namesrv:9876" {
		t.Fatalf("RocketMQ4 endpoint = %q, want namesrv:9876", got)
	}
}

func TestConfigContractRejectsIncompleteOrInvalidProducerSettings(t *testing.T) {
	for _, body := range []string{
		"rocketmq: {}\n",
		"rocketmq:\n  driver: rocketmq5\n  endpoint: namesrv:9876\n",
		"rocketmq:\n  endpoint: namesrv:9876\n  producer_group: legacy\n",
		"mq:\n  endpoint: namesrv:9876\n  group: legacy\n",
		"rocketmq:\n  endpoint: namesrv:9876\n  access_key: ak\n",
		"rocketmq:\n  endpoint: namesrv\n",
		"rocketmq:\n  endpoint: namesrv:9876\n  producer_retry_times: -1\n",
	} {
		if _, err := New(testSnapshot(t, body)); err == nil {
			t.Fatalf("New() error = nil for config:\n%s", body)
		}
	}
}

func TestLifecycleContractOnlyOwnsProducer(t *testing.T) {
	fake := &testProducer{}
	client := &Client{
		cfg: Config{Endpoint: "namesrv:9876"},
		newProducer: func(Config) (producer, error) {
			return fake, nil
		},
	}
	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("start RocketMQ client: %v", err)
	}
	if fake.stops != 0 {
		t.Fatalf("producer stopped during Start: %d", fake.stops)
	}
	if _, err := client.Send(context.Background(), &Message{Topic: "events", Body: []byte("payload")}); err != nil {
		t.Fatalf("send message: %v", err)
	}
	if fake.sends != 1 {
		t.Fatalf("producer sends = %d, want 1", fake.sends)
	}
	if err := client.Stop(context.Background()); err != nil {
		t.Fatalf("stop RocketMQ client: %v", err)
	}
	if fake.stops != 1 {
		t.Fatalf("producer stops = %d, want 1", fake.stops)
	}
}

func TestLifecycleContractStopsProducerAfterInFlightSend(t *testing.T) {
	fake := &testProducer{sendStarted: make(chan struct{}), releaseSend: make(chan struct{})}
	client := &Client{
		cfg:         Config{Endpoint: "namesrv:9876"},
		newProducer: func(Config) (producer, error) { return fake, nil },
	}
	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("start RocketMQ client: %v", err)
	}
	sendDone := make(chan error, 1)
	go func() { _, err := client.Send(context.Background(), &Message{Topic: "events"}); sendDone <- err }()
	<-fake.sendStarted
	stopDone := make(chan error, 1)
	go func() { stopDone <- client.Stop(context.Background()) }()
	waitForState(t, client, stateStopping)
	select {
	case err := <-stopDone:
		t.Fatalf("Stop() returned before send completed: %v", err)
	default:
	}
	if _, err := client.Send(context.Background(), &Message{Topic: "events"}); err == nil {
		t.Fatal("Send() succeeded after Stop started")
	}
	close(fake.releaseSend)
	if err := <-sendDone; err != nil {
		t.Fatalf("in-flight send: %v", err)
	}
	if err := <-stopDone; err != nil {
		t.Fatalf("stop RocketMQ client: %v", err)
	}
	if fake.stops != 1 {
		t.Fatalf("producer stops = %d, want 1", fake.stops)
	}
}

func TestConfigContractDefaultsProducerRetriesToZero(t *testing.T) {
	client, err := New(testSnapshot(t, "rocketmq:\n  endpoint: namesrv:9876\n"))
	if err != nil {
		t.Fatalf("new RocketMQ client: %v", err)
	}
	if got := client.Config().ProducerRetryTimes; got != 0 {
		t.Fatalf("producer retry times = %d, want 0", got)
	}
}

func TestLifecycleContractStopsProducerOnCancelledStart(t *testing.T) {
	fake := &testProducer{}
	client := &Client{
		cfg:         Config{Endpoint: "namesrv:9876"},
		newProducer: func(Config) (producer, error) { return fake, nil },
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := client.Start(ctx); err == nil {
		t.Fatal("Start() error = nil")
	}
	if fake.stops != 0 {
		t.Fatalf("producer should not be created for pre-cancelled context: %d", fake.stops)
	}
}

func TestLifecycleContractRejectsProducerFailure(t *testing.T) {
	client := &Client{
		cfg:         Config{Endpoint: "namesrv:9876"},
		newProducer: func(Config) (producer, error) { return nil, errors.New("broker unavailable") },
	}
	if err := client.Start(context.Background()); err == nil {
		t.Fatal("Start() error = nil")
	}
}

type testProducer struct {
	stops       int
	sends       int
	sendStarted chan struct{}
	releaseSend chan struct{}
}

func (p *testProducer) Send(context.Context, *Message) ([]SendReceipt, error) {
	p.sends++
	if p.sendStarted != nil {
		close(p.sendStarted)
		<-p.releaseSend
	}
	return []SendReceipt{{MessageID: "message-1"}}, nil
}
func (p *testProducer) Stop() error { p.stops++; return nil }

func waitForState(t *testing.T, client *Client, want state) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		client.mu.RLock()
		got := client.state
		client.mu.RUnlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("client state did not become %d", want)
}

func TestRocketMQ4StartFailureShutsDownSDKProducer(t *testing.T) {
	fake := &testRocketMQ4SDKProducer{startErr: errors.New("nameserver unavailable")}
	_, err := newRocketMQ4ProducerWithFactory(Config{Endpoint: "namesrv:9876"}, func(...producer4.Option) (rocketMQ4SDKProducer, error) {
		return fake, nil
	})
	if err == nil {
		t.Fatal("newRocketMQ4ProducerWithFactory() error = nil")
	}
	if fake.shutdowns != 1 {
		t.Fatalf("SDK producer shutdowns = %d, want 1", fake.shutdowns)
	}
}

type testRocketMQ4SDKProducer struct {
	startErr  error
	shutdowns int
}

func (p *testRocketMQ4SDKProducer) Start() error { return p.startErr }
func (p *testRocketMQ4SDKProducer) Shutdown() error {
	p.shutdowns++
	return nil
}
func (*testRocketMQ4SDKProducer) SendSync(context.Context, ...*primitive4.Message) (*primitive4.SendResult, error) {
	return nil, nil
}

func testSnapshot(t *testing.T, body string) config.SourceSnapshot {
	t.Helper()
	path := filepath.Join(t.TempDir(), "infra.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	snapshot, err := config.LoadInputs(context.Background(), config.ConfigInputs{MergedPaths: []string{path}}, "CORE_CASINO_ROCKETMQ_TEST__")
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
