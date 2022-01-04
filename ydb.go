package ydb

import "github.com/ydb-platform/ydb-go-sdk/v3"

func ConnectionString(uri string) (ydb.ConnectParams, error) {
	return ydb.ConnectionString(uri)
}

func IsTimeoutError(err error) bool {
	return ydb.IsTimeoutError(err)
}

func IsTransportError(err error) bool {
	return ydb.IsTransportError(err)
}

func IsTransportErrorCancelled(err error) bool {
	return ydb.IsTransportErrorCancelled(err)
}

func IsTransportErrorResourceExhausted(err error) bool {
	return ydb.IsTransportErrorResourceExhausted(err)
}

func IsOperationError(err error) bool {
	return ydb.IsOperationError(err)
}

func IsOperationErrorOverloaded(err error) bool {
	return ydb.IsOperationErrorOverloaded(err)
}

func IsOperationErrorUnavailable(err error) bool {
	return ydb.IsOperationErrorUnavailable(err)
}

func IsOperationErrorAlreadyExistsError(err error) bool {
	return ydb.IsOperationErrorAlreadyExistsError(err)
}

func IsOperationErrorNotFoundError(err error) bool {
	return ydb.IsOperationErrorNotFoundError(err)
}

func IsOperationErrorSchemeError(err error) bool {
	return ydb.IsOperationErrorSchemeError(err)
}
