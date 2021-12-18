package ydb

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sql/internal/mode"
	"github.com/ydb-platform/ydb-go-sql/internal/x"
)

func WithScanQuery(ctx context.Context) context.Context {
	return x.WithQueryMode(ctx, mode.ScanQuery)
}

func WithDataQuery(ctx context.Context) context.Context {
	return x.WithQueryMode(ctx, mode.DataQuery)
}

func WithSchemeQuery(ctx context.Context) context.Context {
	return x.WithQueryMode(ctx, mode.SchemeQuery)
}

func WithExplain(ctx context.Context) context.Context {
	return x.WithQueryMode(ctx, mode.ExplainQuery)
}

func WithTxControl(ctx context.Context, tx *table.TransactionControl) context.Context {
	return x.WithTxControl(ctx, tx)
}
