package errors

import (
	"database/sql/driver"
	"errors"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

var (
	ErrDeprecated          = errors.New("ydb: deprecated")
	ErrUnsupported         = errors.New("ydb: not supported")
	ErrActiveTransaction   = errors.New("ydb: can not begin tx within active tx")
	ErrNoActiveTransaction = errors.New("ydb: no active tx to work with")
	ErrResultTruncated     = errors.New("ydb: result set has been truncated")
	ErrWrongTxIsolation    = errors.New("ydb: wrong tx isolation")
	ErrExecOnReadOnlyTx    = errors.New("ydb: cannot execute query on read-only tx")

	// Deprecated: not used
	ErrSessionBusy = errors.New("ydb: session is busy")
)

func Map(err error) error {
	if err == nil {
		return nil
	}
	m := retry.Check(err)
	switch {
	case
		m.MustDeleteSession(),
		ydb.IsOperationErrorOverloaded(err),
		ydb.IsOperationErrorUnavailable(err),
		ydb.IsTransportErrorResourceExhausted(err):
		return driver.ErrBadConn
	default:
		return err
	}
}
