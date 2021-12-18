package x

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"

	"github.com/ydb-platform/ydb-go-sql/internal/mode"
)

type (
	ctxTransactionControlKey struct{}
	ctxDataQueryOptionsKey   struct{}
	ctxScanQueryOptionsKey   struct{}
	ctxModeTypeKey           struct{}
)

func WithTxControl(ctx context.Context, txc *table.TransactionControl) context.Context {
	return context.WithValue(ctx, ctxTransactionControlKey{}, txc)
}

func TxControl(ctx context.Context, defaultTxControl *table.TransactionControl) *table.TransactionControl {
	if txc, ok := ctx.Value(ctxTransactionControlKey{}).(*table.TransactionControl); ok {
		return txc
	}
	return defaultTxControl
}

func WithScanQueryOptions(ctx context.Context, opts []options.ExecuteScanQueryOption) context.Context {
	return context.WithValue(ctx, ctxScanQueryOptionsKey{}, append(ScanQueryOptions(ctx), opts...))
}

func ScanQueryOptions(ctx context.Context) []options.ExecuteScanQueryOption {
	if opts, ok := ctx.Value(ctxScanQueryOptionsKey{}).([]options.ExecuteScanQueryOption); ok {
		return opts
	}
	return nil
}

func WithDataQueryOptions(ctx context.Context, opts []options.ExecuteDataQueryOption) context.Context {
	return context.WithValue(ctx, ctxDataQueryOptionsKey{}, append(DataQueryOptions(ctx), opts...))
}

func DataQueryOptions(ctx context.Context) []options.ExecuteDataQueryOption {
	if opts, ok := ctx.Value(ctxDataQueryOptionsKey{}).([]options.ExecuteDataQueryOption); ok {
		return opts
	}
	return nil
}

// WithQueryMode returns a copy of parent context with scan query flag.
func WithQueryMode(ctx context.Context, m mode.Type) context.Context {
	return context.WithValue(ctx, ctxModeTypeKey{}, m)
}

// QueryMode returns true if context contains scan query flag.
func QueryMode(ctx context.Context) mode.Type {
	if m, ok := ctx.Value(ctxModeTypeKey{}).(mode.Type); ok {
		return m
	}
	return mode.Default
}
