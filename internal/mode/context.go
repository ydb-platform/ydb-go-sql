package mode

import (
	"fmt"
)

type Type int

const (
	Default = iota
	ScanQuery
	ExplainQuery
	SchemeQuery

	DataQuery = Default
)

func (t Type) String() string {
	switch t {
	case DataQuery:
		return "data_query"
	case ScanQuery:
		return "scan_query"
	case ExplainQuery:
		return "explain_query"
	case SchemeQuery:
		return "explain_query"
	default:
		return fmt.Sprintf("unknown_query_mode_%d", t)
	}
}
