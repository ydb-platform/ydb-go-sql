package ydb

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"

	"github.com/ydb-platform/ydb-go-sql/internal/x"
)

func WithTxControl(ctx context.Context, tx *table.TransactionControl) context.Context {
	return x.WithTxControl(ctx, tx)
}
