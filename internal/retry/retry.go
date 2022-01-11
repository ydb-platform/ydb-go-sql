package retry

import (
	"context"
	"database/sql/driver"

	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

func WithSession(
	ctx context.Context,
	s table.Session,
	isOperationIdempotent bool,
	op table.Operation,
) (err error) {
	var (
		i    int
		code = int32(0)
	)
	for ; ; i++ {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return

		default:
			if err = op(ctx, s); err == nil {
				return
			}
			m := retry.Check(err)
			if m.StatusCode() != code {
				i = 0
			}
			if m.MustDeleteSession() {
				return driver.ErrBadConn
			}
			if !m.MustRetry(isOperationIdempotent) {
				return
			}
			if err = retry.Wait(ctx, retry.FastBackoff, retry.SlowBackoff, m, i); err != nil {
				return
			}
			code = m.StatusCode()
		}
	}
}

func WithTransaction(
	ctx context.Context,
	tx table.Transaction,
	isOperationIdempotent bool,
	op table.TxOperation,
) (err error) {
	var (
		i    int
		code = int32(0)
	)
	for ; ; i++ {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return

		default:
			if err = op(ctx, tx); err == nil {
				return
			}
			m := retry.Check(err)
			if m.StatusCode() != code {
				i = 0
			}
			if m.MustDeleteSession() {
				return driver.ErrBadConn
			}
			if !m.MustRetry(isOperationIdempotent) {
				return
			}
			if err = retry.Wait(ctx, retry.FastBackoff, retry.SlowBackoff, m, i); err != nil {
				return
			}
			code = m.StatusCode()
		}
	}
}
