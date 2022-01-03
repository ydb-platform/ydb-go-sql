package ydb

import "github.com/ydb-platform/ydb-go-sql/internal/mode"

var (
	dataQueryPrefix    = mode.Prefix + mode.DataQuery.String() + "\n"
	scanQueryPrefix    = mode.Prefix + mode.ScanQuery.String() + "\n"
	schemeQueryPrefix  = mode.Prefix + mode.SchemeQuery.String() + "\n"
	explainQueryPrefix = mode.Prefix + mode.ExplainQuery.String() + "\n"
)

// DataQuery prepends to query "--gosql:DATA\n" for routing
// queries on driver-side
//
// This is temporary feature which will removed in the future
// when YDB API will support query routing on server side
func DataQuery(query string) string {
	return dataQueryPrefix + query
}

// ScanQuery prepends to query "--gosql:SCAN\n" for routing
// queries on driver-side
//
// This is temporary feature which will removed in the future
// when YDB API will support query routing on server side
func ScanQuery(query string) string {
	return scanQueryPrefix + query
}

// SchemeQuery prepends to query "--gosql:SCHEME\n" for routing
// queries on driver-side
//
// This is temporary feature which will removed in the future
// when YDB API will support query routing on server side
func SchemeQuery(query string) string {
	return schemeQueryPrefix + query
}

// ExplainQuery prepends to query "--gosql:EXPLAIN\n" for routing
// queries on driver-side
//
// This is temporary feature which will removed in the future
// when YDB API will support query routing on server side
func ExplainQuery(query string) string {
	return explainQueryPrefix + query
}
