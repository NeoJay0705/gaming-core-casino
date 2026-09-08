package redis

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	redisclient "github.com/redis/go-redis/v9"
)

func TestConfigContractInfersSingleAndClusterClients(t *testing.T) {
	single, err := New(testSnapshot(t, `redis:
  addr: 127.0.0.1:6379
  pool_size: 12
  min_idle_conns: 3
  dial_timeout: 1s
`))
	if err != nil {
		t.Fatalf("new single client: %v", err)
	}
	if cfg := single.Config(); cfg.Addr != "127.0.0.1:6379" || len(cfg.Addrs) != 0 || cfg.PoolSize != 12 || cfg.MinIdleConns != 3 {
		t.Fatalf("single config = %#v", cfg)
	}

	cluster, err := New(testSnapshot(t, `redis:
  addrs: [redis-0:6379, redis-1:6379]
  pool_size: 20
`))
	if err != nil {
		t.Fatalf("new cluster client: %v", err)
	}
	if cfg := cluster.Config(); cfg.Addr != "" || len(cfg.Addrs) != 2 || cfg.PoolSize != 20 {
		t.Fatalf("cluster config = %#v", cfg)
	}
	if _, ok := newUniversalClient(single.Config()).(*redisclient.Client); !ok {
		t.Fatal("single config did not build *redis.Client")
	}
	if _, ok := newUniversalClient(cluster.Config()).(*redisclient.ClusterClient); !ok {
		t.Fatal("cluster config did not build *redis.ClusterClient")
	}

	copy := cluster.Config()
	copy.Addrs[0] = "mutated:6379"
	if got := cluster.Config().Addrs[0]; got != "redis-0:6379" {
		t.Fatalf("Config() leaked mutable addrs: %q", got)
	}
}

func TestConfigContractRejectsInvalidModeSettings(t *testing.T) {
	for _, body := range []string{
		"redis: {}\n",
		"redis:\n  addr: redis:6379\n  addrs: [redis-0:6379]\n",
		"redis:\n  addrs: [redis:6379]\n  db: 1\n",
		"redis:\n  addr: redis:6379\n  min_idle_conns: 2\n  max_idle_conns: 1\n",
	} {
		if _, err := New(testSnapshot(t, body)); err == nil {
			t.Fatalf("New() error = nil for config:\n%s", body)
		}
	}
}

func TestLifecycleContractPingsAndClosesSingleConnectionPool(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go serveRESPPing(listener)

	client, err := New(testSnapshot(t, fmt.Sprintf("redis:\n  addr: %q\n  pool_size: 7\n", listener.Addr().String())))
	if err != nil {
		t.Fatalf("new redis client: %v", err)
	}
	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("start redis client: %v", err)
	}
	if _, err := client.Client(); err != nil {
		t.Fatalf("started client unavailable: %v", err)
	}
	if err := client.Stop(context.Background()); err != nil {
		t.Fatalf("stop redis client: %v", err)
	}
	if _, err := client.Client(); err == nil {
		t.Fatal("stopped client remained available")
	}
}

func serveRESPPing(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go func() {
			defer conn.Close()
			reader := bufio.NewReader(conn)
			for {
				command, err := readRESPCommand(reader)
				if err != nil {
					return
				}
				response := "+OK\r\n"
				if strings.EqualFold(command, "hello") {
					// Redis < 6 does not implement HELLO. go-redis falls back to
					// RESP2 after this error, which keeps this test server minimal.
					response = "-ERR unknown command 'hello'\r\n"
				} else if strings.EqualFold(command, "ping") {
					response = "+PONG\r\n"
				}
				if _, err := conn.Write([]byte(response)); err != nil {
					return
				}
			}
		}()
	}
}

func readRESPCommand(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) < 4 || line[0] != '*' {
		return "", fmt.Errorf("invalid RESP array %q", line)
	}
	count, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil || count <= 0 {
		return "", fmt.Errorf("invalid RESP array length %q", line)
	}
	var command string
	for i := 0; i < count; i++ {
		lengthLine, err := reader.ReadString('\n')
		if err != nil {
			return "", err
		}
		if len(lengthLine) < 4 || lengthLine[0] != '$' {
			return "", fmt.Errorf("invalid RESP bulk length %q", lengthLine)
		}
		length, err := strconv.Atoi(strings.TrimSpace(lengthLine[1:]))
		if err != nil || length < 0 {
			return "", fmt.Errorf("invalid RESP bulk length %q", lengthLine)
		}
		value := make([]byte, length+2)
		if _, err := io.ReadFull(reader, value); err != nil {
			return "", err
		}
		if i == 0 {
			command = string(value[:length])
		}
	}
	return command, nil
}

func testSnapshot(t *testing.T, body string) config.SourceSnapshot {
	t.Helper()
	path := filepath.Join(t.TempDir(), "infra.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	snapshot, err := config.LoadInputs(context.Background(), config.ConfigInputs{MergedPaths: []string{path}}, "CORE_CASINO_REDIS_TEST__")
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
