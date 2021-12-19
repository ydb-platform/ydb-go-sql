package connector

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"testing"
	"time"
)

func TestConnectorDialOnPing(t *testing.T) {
	const timeout = time.Second

	client, server := net.Pipe()
	defer func() {
		_ = server.Close()
	}()

	dial := make(chan struct{})
	c := New(
		nil,
		WithEndpoint("127.0.0.1:9999"),
		WithDatabase("/local"),
		withNetDial(func(_ context.Context, addr string) (net.Conn, error) {
			dial <- struct{}{}
			return client, nil
		}),
		WithAnonymousCredentials(),
	)

	db := sql.OpenDB(c)
	select {
	case <-dial:
		t.Fatalf("unexpected dial")
	case <-time.After(timeout):
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = db.PingContext(ctx)
	}()

	select {
	case <-dial:
	case <-time.After(timeout):
		t.Fatalf("no dial after %s", timeout)
	}
}

// TestConnectorRedialOnError checks re-dialing
// KIKIMR-8592: check that we try re-dial after any error
func TestConnectorRedialOnError(t *testing.T) {
	const timeout = 100 * time.Millisecond

	client, server := net.Pipe()
	defer func() {
		_ = server.Close()
	}()
	success := make(chan bool, 1)

	dial := false
	c := New(
		nil,
		WithEndpoint("127.0.0.1:9999"),
		WithDatabase("/local"),
		withNetDial(func(_ context.Context, addr string) (net.Conn, error) {
			dial = true
			select {
			case <-success:
				// it will still fails on grpc dial
				return client, nil
			default:
				return nil, errors.New("any error")
			}
		}),
		WithAnonymousCredentials(),
	)

	db := sql.OpenDB(c)
	for i := 0; i < 3; i++ {
		success <- i%2 == 0
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		_ = db.PingContext(ctx)
		cancel()
		if !dial {
			t.Fatalf("no dial on re-ping at %v iteration", i)
		}
		dial = false
	}
}
