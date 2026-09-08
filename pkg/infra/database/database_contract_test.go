package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
	"gorm.io/gorm"
)

func TestConfigContractValidatesPoolSettings(t *testing.T) {
	client, err := New(testSnapshot(t, `database:
  dsn: user:pass@tcp(tidb:4000)/gaming?parseTime=true
  max_open_conns: 20
  max_idle_conns: 5
  conn_max_lifetime: 20m
  conn_max_idle_time: 3m
`))
	if err != nil {
		t.Fatalf("new database client: %v", err)
	}
	if cfg := client.Config(); cfg.MaxOpenConns != 20 || cfg.MaxIdleConns != 5 || cfg.ConnMaxLifetime.String() != "20m0s" {
		t.Fatalf("database config = %#v", cfg)
	}
	for _, body := range []string{
		"database: {}\n",
		"database:\n  dsn: x\n  max_open_conns: 1\n  max_idle_conns: 2\n",
		"database:\n  dsn: x\n  conn_max_lifetime: -1s\n",
	} {
		if _, err := New(testSnapshot(t, body)); err == nil {
			t.Fatalf("New() error = nil for config:\n%s", body)
		}
	}
}

func TestLifecycleContractPingsConfiguresAndClosesPool(t *testing.T) {
	sqlDB := sql.OpenDB(testConnector{})
	client := &Client{
		cfg: Config{DSN: "test", MaxOpenConns: 6, MaxIdleConns: 2, ConnMaxLifetime: defaultConnMaxLifetime},
		open: func(Config) (*gorm.DB, *sql.DB, error) {
			return &gorm.DB{}, sqlDB, nil
		},
	}
	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("start database client: %v", err)
	}
	if got, err := client.DB(); err != nil || got == nil {
		t.Fatalf("started DB = %v, %v", got, err)
	}
	if err := client.Stop(context.Background()); err != nil {
		t.Fatalf("stop database client: %v", err)
	}
	if _, err := client.DB(); err == nil {
		t.Fatal("stopped DB remained available")
	}
}

func TestLifecycleContractClosesPoolAfterPingFailure(t *testing.T) {
	sqlDB := sql.OpenDB(testConnector{pingErr: errors.New("unavailable")})
	client := &Client{
		cfg:  Config{DSN: "test", ConnMaxLifetime: defaultConnMaxLifetime},
		open: func(Config) (*gorm.DB, *sql.DB, error) { return &gorm.DB{}, sqlDB, nil },
	}
	if err := client.Start(context.Background()); err == nil {
		t.Fatal("start error = nil")
	}
	if err := sqlDB.PingContext(context.Background()); err == nil {
		t.Fatal("pool was not closed after failed start")
	}
}

type testConnector struct{ pingErr error }

func (c testConnector) Connect(context.Context) (driver.Conn, error) {
	return testConn{pingErr: c.pingErr}, nil
}
func (testConnector) Driver() driver.Driver { return testDriver{} }

type testDriver struct{}

func (testDriver) Open(string) (driver.Conn, error) { return testConn{}, nil }

type testConn struct{ pingErr error }

func (c testConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not implemented") }
func (c testConn) Close() error                        { return nil }
func (c testConn) Begin() (driver.Tx, error)           { return nil, errors.New("not implemented") }
func (c testConn) Ping(context.Context) error          { return c.pingErr }

func testSnapshot(t *testing.T, body string) config.SourceSnapshot {
	t.Helper()
	path := filepath.Join(t.TempDir(), "infra.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	snapshot, err := config.LoadInputs(context.Background(), config.ConfigInputs{MergedPaths: []string{path}}, "CORE_CASINO_DATABASE_TEST__")
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
