package stmt

import (
	"context"
	"database/sql/driver"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"

	"github.com/ydb-platform/ydb-go-sql/internal/check"
	"github.com/ydb-platform/ydb-go-sql/internal/errors"
	"github.com/ydb-platform/ydb-go-sql/internal/nop"
	"github.com/ydb-platform/ydb-go-sql/internal/rows"
	"github.com/ydb-platform/ydb-go-sql/internal/x"
)

type Stmt interface {
	driver.Stmt
	driver.StmtQueryContext
	driver.StmtExecContext

	driver.NamedValueChecker
}

type stmt struct {
	stmt             table.Statement
	defaultTxControl *table.TransactionControl
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	_, res, err := s.stmt.Execute(ctx, x.TxControl(ctx, s.defaultTxControl), x.ToQueryParams(args), x.DataQueryOptions(ctx)...)
	if err != nil {
		return nil, errors.Map(err)
	}
	return rows.Result(res), errors.Map(res.Err())
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	_, res, err := s.stmt.Execute(ctx, x.TxControl(ctx, s.defaultTxControl), x.ToQueryParams(args), x.DataQueryOptions(ctx)...)
	if err != nil {
		return nil, errors.Map(err)
	}
	return nop.Result(), errors.Map(res.Err())
}

func New(
	s table.Statement,
	defaultTxControl *table.TransactionControl,
) Stmt {
	return &stmt{
		stmt:             s,
		defaultTxControl: defaultTxControl,
	}
}

func (s *stmt) NumInput() int {
	return s.stmt.NumInput()
}

func (s *stmt) Close() error {
	return nil
}

func (s stmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errors.ErrDeprecated
}

func (s stmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, errors.ErrDeprecated
}

func (s *stmt) CheckNamedValue(v *driver.NamedValue) error {
	return check.NamedValue(v)
}
