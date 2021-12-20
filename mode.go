package ydb

import "github.com/ydb-platform/ydb-go-sql/internal/mode"

// DataQuery prepends to query "--ydb:DATA\n"
func DataQuery(query string) string {
	return mode.Prefix + mode.DataQuery.String() + "\n" + query
}

// ScanQuery prepends to query "--ydb:SCAN\n"
func ScanQuery(query string) string {
	return mode.Prefix + mode.ScanQuery.String() + "\n" + query
}

// SchemeQuery prepends to query "--ydb:SCHEME\n"
func SchemeQuery(query string) string {
	return mode.Prefix + mode.SchemeQuery.String() + "\n" + query
}

// ExplainQuery prepends to query "--ydb:EXPLAIN\n"
func ExplainQuery(query string) string {
	return mode.Prefix + mode.ExplainQuery.String() + "\n" + query
}
