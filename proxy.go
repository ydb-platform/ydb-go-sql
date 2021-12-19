package ydb

// file proxy.go contains proxy methods to ydb native public methods
// for exclude import native ydb-go-sdk package

import (
	"github.com/ydb-platform/ydb-go-sdk/v3"
)

func ConnectionString(uri string) (ydb.ConnectParams, error) {
	return ydb.ConnectionString(uri)
}
