package nop

import (
	"database/sql/driver"

	"github.com/ydb-platform/ydb-go-sql/internal/errors"
)

type result struct {
	lastInsertID *int64
	rowsAffected *int64
}

type option func(r *result)

func WithResultLastInsertID(id int64) option {
	return func(r *result) {
		r.lastInsertID = &id
	}
}

func WithResultRowsAffected(rowsAffected int64) option {
	return func(r *result) {
		r.rowsAffected = &rowsAffected
	}
}

func (r *result) LastInsertId() (int64, error) {
	if r.lastInsertID != nil {
		return *r.lastInsertID, nil
	}
	return 0, errors.ErrUnsupported
}

func (r *result) RowsAffected() (int64, error) {
	if r.rowsAffected != nil {
		return *r.rowsAffected, nil
	}
	return 0, errors.ErrUnsupported
}

func Result(opts ...option) driver.Result {
	r := &result{}
	for _, o := range opts {
		o(r)
	}
	return r
}
