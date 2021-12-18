package single

import (
	"database/sql"
	"database/sql/driver"
	"io"
)

type Rows interface {
	driver.Rows
}

type single struct {
	values []sql.NamedArg
}

func (r *single) Columns() (columns []string) {
	for _, v := range r.values {
		columns = append(columns, v.Name)
	}
	return columns
}

func (r *single) Close() error {
	return nil
}

func (r *single) Next(dst []driver.Value) error {
	if r.values == nil {
		return io.EOF
	}
	for i := range r.values {
		dst[i] = r.values[i].Value
	}
	r.values = nil
	return nil
}

func Result(values ...sql.NamedArg) Rows {
	return &single{
		values: values,
	}
}
