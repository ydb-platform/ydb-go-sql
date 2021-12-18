package conn

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"

	"github.com/ydb-platform/ydb-go-sql/internal/check"
	"github.com/ydb-platform/ydb-go-sql/internal/errors"
	"github.com/ydb-platform/ydb-go-sql/internal/mode"
	"github.com/ydb-platform/ydb-go-sql/internal/nop"
	"github.com/ydb-platform/ydb-go-sql/internal/rows"
	"github.com/ydb-platform/ydb-go-sql/internal/single"
	"github.com/ydb-platform/ydb-go-sql/internal/stmt"
	"github.com/ydb-platform/ydb-go-sql/internal/stream"
	"github.com/ydb-platform/ydb-go-sql/internal/tx"
	"github.com/ydb-platform/ydb-go-sql/internal/x"
)

type Conn interface {
	driver.Conn
	driver.QueryerContext
	driver.ExecerContext

	driver.NamedValueChecker
}

// conn is a connection to the ydb.
type conn struct {
	s  table.ClosableSession // Immutable and r/o usage.
	tx tx.Tx

	defaultTxControl *table.TransactionControl
	dataOpts         []options.ExecuteDataQueryOption
	scanOpts         []options.ExecuteScanQueryOption

	idle bool
}

func New(s table.ClosableSession, opts ...Option) Conn {
	c := &conn{s: s}
	for _, o := range opts {
		o(c)
	}
	return c
}

func (c *conn) ResetSession(ctx context.Context) error {
	if c.idle {
		return nil
	}
	c.idle = true
	return nil
}

func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	s, err := c.s.Prepare(ctx, query)
	if err != nil {
		return nil, errors.Map(err)
	}
	return stmt.New(s, c.defaultTxControl), nil
}

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (_ driver.Tx, err error) {
	if c.tx != nil {
		return nil, errors.ErrActiveTransaction
	}
	c.tx, err = tx.New(ctx, opts, c.s, func() { c.tx = nil })
	return c.tx, err
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.tx != nil {
		return c.tx.ExecContext(ctx, query, args)
	}
	switch m := x.QueryMode(ctx); m {
	case mode.DataQuery:
		_, res, err := c.s.Execute(ctx, x.TxControl(ctx, c.defaultTxControl), query, x.ToQueryParams(args))
		if err != nil {
			return nil, errors.Map(err)
		}
		return nop.Result(), errors.Map(res.Err())
	case mode.SchemeQuery:
		err := c.s.ExecuteSchemeQuery(ctx, query, x.ToSchemeOptions(args)...)
		if err != nil {
			return nil, errors.Map(err)
		}
		return nop.Result(), nil
	default:
		return nil, fmt.Errorf("unsupported query mode %s type for execute query", m)
	}
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.tx != nil {
		return c.tx.QueryContext(ctx, query, args)
	}
	switch m := x.QueryMode(ctx); m {
	case mode.DataQuery:
		_, res, err := c.s.Execute(ctx, x.TxControl(ctx, c.defaultTxControl), query, x.ToQueryParams(args))
		if err != nil {
			return nil, errors.Map(err)
		}
		return rows.Result(res), errors.Map(res.Err())
	case mode.ScanQuery:
		res, err := c.s.StreamExecuteScanQuery(ctx, query, x.ToQueryParams(args), x.ScanQueryOptions(ctx)...)
		if err != nil {
			return nil, errors.Map(err)
		}
		return stream.Result(ctx, res), errors.Map(res.Err())
	case mode.ExplainQuery:
		exp, err := c.s.Explain(ctx, query)
		if err != nil {
			return nil, errors.Map(err)
		}
		return single.Result(
			sql.Named("AST", exp.AST),
			sql.Named("Plan", exp.Plan),
		), nil
	default:
		return nil, fmt.Errorf("unsupported query mode %s type on conn query", m)
	}
}

func (c *conn) CheckNamedValue(v *driver.NamedValue) error {
	return check.NamedValue(v)
}

func (c *conn) Ping(ctx context.Context) error {
	return errors.Map(c.s.KeepAlive(ctx))
}

func (c *conn) Close() error {
	ctx := context.Background()
	err := c.s.Close(ctx)
	return errors.Map(err)
}

func (c *conn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.ErrDeprecated
}

func (c *conn) Begin() (driver.Tx, error) {
	return nil, errors.ErrDeprecated
}
