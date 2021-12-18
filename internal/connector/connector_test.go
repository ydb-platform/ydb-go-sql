package connector

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
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

func TestConnectorWithQueryCachePolicyKeepInCache(t *testing.T) {
	for _, test := range [...]struct {
		name                   string
		cacheSize              int
		prepareCount           int
		prepareRequestsCount   int
		queryCachePolicyOption []options.QueryCachePolicyOption
	}{
		{
			name:                   "fixed query cache size, with server cache, one request proxed to server",
			cacheSize:              10,
			prepareCount:           10,
			prepareRequestsCount:   1,
			queryCachePolicyOption: []options.QueryCachePolicyOption{options.WithQueryCachePolicyKeepInCache()},
		},
		{
			name:                   "default query cache size, with server cache, one request proxed to server",
			cacheSize:              0,
			prepareCount:           10,
			prepareRequestsCount:   1,
			queryCachePolicyOption: []options.QueryCachePolicyOption{options.WithQueryCachePolicyKeepInCache()},
		},
		{
			name:                   "disabled query cache, with server cache, all requests proxed to server",
			cacheSize:              -1,
			prepareCount:           10,
			prepareRequestsCount:   10,
			queryCachePolicyOption: []options.QueryCachePolicyOption{options.WithQueryCachePolicyKeepInCache()},
		},
		{
			name:                   "fixed query cache size, no server cache, one request proxed to server",
			cacheSize:              10,
			prepareCount:           10,
			prepareRequestsCount:   1,
			queryCachePolicyOption: []options.QueryCachePolicyOption{},
		},
		{
			name:                   "default query cache size, no server cache, one request proxed to server",
			cacheSize:              0,
			prepareCount:           10,
			prepareRequestsCount:   1,
			queryCachePolicyOption: []options.QueryCachePolicyOption{},
		},
		{
			name:                   "disabled query cache, no server cache, all requests proxed to server",
			cacheSize:              -1,
			prepareCount:           10,
			prepareRequestsCount:   10,
			queryCachePolicyOption: []options.QueryCachePolicyOption{},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, server := net.Pipe()
			defer func() {
				_ = client.Close()
			}()
			defer func() {
				_ = server.Close()
			}()
			c := New(
				nil,
				With(
					config.WithGrpcOptions(
						grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) (err error) {
							switch m := testutil.Method(method).Code(); m {
							case testutil.TableCreateSession:
								r, ok := (reply).(*Ydb_Table.CreateSessionResponse)
								if !ok {
									t.Fatalf("Unexpected response type %T", reply)
								}
								r.Operation.Result, err = anypb.New(
									&Ydb_Table.CreateSessionResult{
										SessionId: testutil.SessionID(),
									},
								)
								return err
							case testutil.TableExecuteDataQuery:
								{
									r, ok := (req).(*Ydb_Table.ExecuteDataQueryRequest)
									if !ok {
										t.Fatalf("Unexpected request type %T", req)
									}
									if len(test.queryCachePolicyOption) > 0 {
										keepInCache := r.QueryCachePolicy.KeepInCache
										if !keepInCache {
											t.Fatalf("wrong keepInCache: %v", keepInCache)
										}
									} else {
										keepInCache := r.QueryCachePolicy.KeepInCache
										if keepInCache {
											t.Fatalf("wrong keepInCache: %v", keepInCache)
										}
									}
								}
								{
									r, ok := (reply).(*Ydb_Table.ExecuteDataQueryResponse)
									if !ok {
										t.Fatalf("Unexpected response type %T", reply)
									}
									r.Operation.Result, err = anypb.New(
										&Ydb_Table.ExecuteQueryResult{
											TxMeta: &Ydb_Table.TransactionMeta{
												Id: "",
											},
										},
									)
									if err != nil {
										t.Fatalf("any proto failed: %v", err)
									}
									return nil
								}
							default:
								t.Fatalf("Unexpected method %d", m)
							}
							return nil
						}),
					),
				),
				WithDefaultExecDataQueryOption(options.WithQueryCachePolicy(test.queryCachePolicyOption...)),
			)
			db := sql.OpenDB(c)
			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			defer cancel()
			rows, err := db.QueryContext(ctx, "SELECT 1")
			if err != nil {
				t.Fatalf("query context failed: %v", err)
			}
			if rows == nil {
				t.Fatalf("unexpected rows result: %v", rows)
			}
		})
	}
}
