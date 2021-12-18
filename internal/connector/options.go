package connector

import (
	"context"
	"net"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Option func(*connector)

func With(options ...config.Option) Option {
	return func(c *connector) {
		c.options = append(c.options, ydb.With(options...))
	}
}

func withNetDial(netDial func(ctx context.Context, address string) (conn net.Conn, err error)) Option {
	return func(c *connector) {
		c.options = append(c.options, ydb.With(config.WithGrpcOptions(grpc.WithContextDialer(netDial))))
	}
}

func WithConnectionString(connection string) Option {
	return func(c *connector) {
		c.options = append(c.options, ydb.WithConnectionString(connection))
	}
}

func WithEndpoint(endpoint string) Option {
	return func(c *connector) {
		c.options = append(c.options, ydb.WithEndpoint(endpoint))
	}
}

func WithCredentials(creds credentials.Credentials) Option {
	return func(c *connector) {
		c.options = append(c.options, ydb.WithCredentials(creds))
	}
}

func WithAnonymousCredentials() Option {
	return func(c *connector) {
		c.options = append(c.options, ydb.WithAnonymousCredentials())
	}
}

func WithAccessTokenCredentials(accessToken string) Option {
	return func(c *connector) {
		c.options = append(c.options, ydb.WithAccessTokenCredentials(accessToken))
	}
}

func WithDatabase(database string) Option {
	return func(c *connector) {
		c.options = append(c.options, ydb.WithDatabase(database))
	}
}

func WithTraceDriver(t trace.Driver) Option {
	return func(c *connector) {
		c.options = append(c.options, ydb.WithTraceDriver(t))
	}
}

func WithTraceTable(t trace.Table) Option {
	return func(c *connector) {
		c.options = append(c.options, ydb.WithTraceTable(t))
	}
}

func WithDefaultExecDataQueryOption(opts ...options.ExecuteDataQueryOption) Option {
	return func(c *connector) {
		c.dataOpts = append(c.dataOpts, opts...)
	}
}

func WithDefaultExecScanQueryOption(opts ...options.ExecuteScanQueryOption) Option {
	return func(c *connector) {
		c.scanOpts = append(c.scanOpts, opts...)
	}
}
