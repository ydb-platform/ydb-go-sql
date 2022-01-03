package ydb

import "github.com/ydb-platform/ydb-go-sdk/v3/table/types"

func Uint64(v uint64) types.Value {
	return types.Uint64Value(v)
}
