package conn

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

type Option func(*conn)

func WithDataOpts(dataOpts []options.ExecuteDataQueryOption) Option {
	return func(c *conn) {
		c.dataOpts = dataOpts
	}
}

func WithScanOpts(scanOpts []options.ExecuteScanQueryOption) Option {
	return func(c *conn) {
		c.scanOpts = scanOpts
	}
}

func WithDefaultTxControl(defaultTxControl *table.TransactionControl) Option {
	return func(c *conn) {
		c.defaultTxControl = defaultTxControl
	}
}
