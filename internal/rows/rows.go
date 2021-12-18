package rows

import (
	"context"
	"database/sql/driver"
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"

	"github.com/ydb-platform/ydb-go-sql/internal/errors"
	"github.com/ydb-platform/ydb-go-sql/internal/x"
)

// Rows is an aggregate interface which returns from rows.Result()
type Rows interface {
	driver.Rows
	driver.RowsNextResultSet
}

type rows struct {
	res result.Result
}

// Result returns Rows interface based on result.Result
func Result(res result.Result) Rows {
	return &rows{
		res: res,
	}
}

func (r *rows) LastInsertId() (int64, error) { return 0, errors.ErrUnsupported }
func (r *rows) RowsAffected() (int64, error) { return 0, errors.ErrUnsupported }

func (r *rows) Columns() []string {
	var i int
	cs := make([]string, r.res.CurrentResultSet().ColumnCount())
	r.res.CurrentResultSet().Columns(func(m options.Column) {
		cs[i] = m.Name
		i++
	})
	return cs
}

func (r *rows) NextResultSet() error {
	if !r.res.NextResultSet(context.Background()) {
		return io.EOF
	}
	return nil
}

func (r *rows) HasNextResultSet() bool {
	return r.res.HasNextResultSet()
}

func (r *rows) Next(dst []driver.Value) (err error) {
	if !r.res.NextRow() {
		return io.EOF
	}
	values := make([]interface{}, len(dst))
	for i := range dst {
		values[i] = x.V()
	}
	if err = r.res.Scan(values...); err != nil {
		return err
	}
	for i := range values {
		s := values[i].(x.Valuer)
		dst[i] = s.Value()
	}
	return r.res.Err()
}

func (r *rows) Close() error {
	return r.res.Close()
}
