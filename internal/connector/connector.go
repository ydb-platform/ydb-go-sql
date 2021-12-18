package connector

import (
	"context"
	"database/sql/driver"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"

	"github.com/ydb-platform/ydb-go-sql/internal/conn"
)

type Connector interface {
	driver.Connector

	Close(ctx context.Context) error
}

type Driver interface {
	driver.Driver

	Done() <-chan struct{}
}

func New(owner Driver, opts ...Option) Connector {
	c := &connector{
		owner: owner,
		defaultTxControl: table.TxControl(
			table.BeginTx(
				table.WithSerializableReadWrite(),
			),
			table.CommitTx(),
		),
	}
	for _, opt := range opts {
		opt(c)
	}
	go func() {
		<-owner.Done()
		c.Close(context.Background())
	}()
	return c
}

// USE CONNECTOR ONLY
type connector struct {
	owner driver.Driver

	options []ydb.Option

	mu sync.RWMutex
	db ydb.Connection

	defaultTxControl *table.TransactionControl

	dataOpts []options.ExecuteDataQueryOption
	scanOpts []options.ExecuteScanQueryOption
}

func (c *connector) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.db == nil {
		return nil
	}
	return c.db.Close(ctx)
}

func (c *connector) init(ctx context.Context) (err error) {
	// in driver database/conn/conn.go:1228 connect run under mutex, but don't rely on it here
	c.mu.Lock()
	defer c.mu.Unlock()
	// in driver database/conn/conn.go:1228 connect run under mutex, but don't rely on it here

	// Setup some more on less generic reasonable pool limit to prevent
	// session overflow on the YDB servers.
	//
	// Note that it must be controlled from outside by making
	// database/conn.DB.SetMaxIdleConns() call. Unfortunately, we can not
	// receive that limit here and we do not want to force user to
	// configure it twice (and pass it as an option to connector).
	if c.db == nil {
		c.db, err = ydb.New(ctx, c.options...)
	}
	return
}

func (c *connector) Connect(ctx context.Context) (_ driver.Conn, err error) {
	if err = c.init(ctx); err != nil {
		return nil, err
	}
	var (
		s table.ClosableSession
	)
	err = retry.Retry(ctx, true, func(ctx context.Context) (err error) {
		c.mu.RLock()
		s, err = c.db.Table().CreateSession(ctx)
		c.mu.RUnlock()
		return err
	})
	if err == nil {
		if s == nil {
			panic("ydb: abnormal result of pool.Create()")
		}
		return conn.New(
			s,
			conn.WithDefaultTxControl(c.defaultTxControl),
			conn.WithDataOpts(c.dataOpts),
			conn.WithScanOpts(c.scanOpts),
		), nil
	}
	return nil, err
}

func (c *connector) Driver() driver.Driver {
	return c.owner
}
