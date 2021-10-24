package ydbsql

import (
	"context"
	"database/sql/driver"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type sqlOption func(*sqlConnector)

func With(options ...ydb.Option) sqlOption {
	return func(c *sqlConnector) {
		c.options = append(c.options, options...)
	}
}

func WithDefaultTxControl(txControl *table.TransactionControl) sqlOption {
	return func(c *sqlConnector) {
		c.txControl = txControl
	}
}

func WithDefaultExecDataQueryOption(opts ...options.ExecuteDataQueryOption) sqlOption {
	return func(c *sqlConnector) {
		c.dataOpts = append(c.dataOpts, opts...)
	}
}

func WithDefaultExecScanQueryOption(opts ...options.ExecuteScanQueryOption) sqlOption {
	return func(c *sqlConnector) {
		c.scanOpts = append(c.scanOpts, opts...)
	}
}

func Connector(options ...sqlOption) (driver.Connector, error) {
	c := &sqlConnector{
		txControl: table.TxControl(
			table.BeginTx(
				table.WithSerializableReadWrite(),
			),
			table.CommitTx(),
		),
	}
	for _, o := range options {
		o(c)
	}
	return c, nil
}

// USE CONNECTOR ONLY
type sqlConnector struct {
	options []ydb.Option

	txControl *table.TransactionControl

	dataOpts []options.ExecuteDataQueryOption
	scanOpts []options.ExecuteScanQueryOption
}

func (c *sqlConnector) Connect(ctx context.Context) (_ driver.Conn, err error) {
	var db ydb.Connection
	db, err = ydb.New(
		ctx,
		append(
			c.options,
			ydb.WithTableConfigOption(
				config.WithTrace(
					trace.Table{
						OnPoolClose: func(info trace.PoolCloseStartInfo) func(trace.PoolCloseDoneInfo) {
							closeCtx := info.Context
							return func(info trace.PoolCloseDoneInfo) {
								go db.Close(closeCtx)
							}
						},
					},
				),
			),
		)...,
	)
	if err != nil {
		return nil, err
	}
	return &sqlConn{
		connector: c,
		client:    db.Table(),
		txControl: c.txControl,
		dataOpts:  c.dataOpts,
		scanOpts:  c.scanOpts,
	}, nil
}

func (c *sqlConnector) Driver() driver.Driver {
	return &sqlDriver{c}
}

// sqlDriver is an adapter to allow the use table client as sql.sqlDriver instance.
// The main purpose of this types is exported is an ability to call Unwrap()
// method on it to receive raw *table.client instance.
type sqlDriver struct {
	c *sqlConnector
}

// Open returns a new connection to the ydb.
func (d *sqlDriver) Open(string) (driver.Conn, error) {
	return nil, ErrDeprecated
}

func (d *sqlDriver) OpenConnector(string) (driver.Connector, error) {
	return d.c, nil
}
