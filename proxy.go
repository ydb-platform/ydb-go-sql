package ydb

import "github.com/ydb-platform/ydb-go-sdk/v3"

func ConnectionString(uri string) (ydb.ConnectParams, error) {
	return ydb.ConnectionString(uri)
}
