package ydb

import (
	"database/sql/driver"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"github.com/ydb-platform/ydb-go-sql/internal/connector"
)

func Connector(opts ...connector.Option) driver.Connector {
	return connector.New(legacyDriver, opts...)
}

func With(opts ...config.Option) connector.Option {
	return connector.With(opts...)
}

func WithConnectionString(connection string) connector.Option {
	return connector.WithConnectionString(connection)
}

func WithDiscoveryInterval(discoveryInterval time.Duration) connector.Option {
	return connector.WithDiscoveryInterval(discoveryInterval)
}

func WithEndpoint(endpoint string) connector.Option {
	return connector.WithEndpoint(endpoint)
}

func WithCredentials(creds credentials.Credentials) connector.Option {
	return connector.WithCredentials(creds)
}

func WithAnonymousCredentials() connector.Option {
	return connector.WithAnonymousCredentials()
}

func WithAccessTokenCredentials(accessToken string) connector.Option {
	return connector.WithAccessTokenCredentials(accessToken)
}

func WithDatabase(database string) connector.Option {
	return connector.WithDatabase(database)
}

func WithTraceDriver(t trace.Driver) connector.Option {
	return connector.WithTraceDriver(t)
}

func WithTraceTable(t trace.Table) connector.Option {
	return connector.WithTraceTable(t)
}

func WithDefaultExecDataQueryOption(opts ...options.ExecuteDataQueryOption) connector.Option {
	return connector.WithDefaultExecDataQueryOption(opts...)
}

func WithDefaultExecScanQueryOption(opts ...options.ExecuteScanQueryOption) connector.Option {
	return connector.WithDefaultExecScanQueryOption(opts...)
}
