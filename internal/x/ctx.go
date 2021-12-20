package x

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

type (
	ctxTransactionControlKey struct{}
	ctxDataQueryOptionsKey   struct{}
	ctxScanQueryOptionsKey   struct{}
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
