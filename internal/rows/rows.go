package rows

import (
	"context"
	"database/sql/driver"
	"github.com/ydb-platform/ydb-go-sql/internal/errors"
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"

	"github.com/ydb-platform/ydb-go-sql/internal/ydb2sql"
)

type Rows interface {
	driver.Rows
	driver.RowsNextResultSet
}

type rows struct {
	res result.Result
}

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
		values[i] = ydb2sql.New()
	}
	if err = r.res.Scan(values...); err != nil {
		return err
	}
	for i := range values {
		s := values[i].(ydb2sql.Valuer)
		dst[i] = s.Value()
	}
	return r.res.Err()
}

func (r *rows) Close() error {
	return r.res.Close()
}
