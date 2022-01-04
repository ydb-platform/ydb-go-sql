package mode

import "strings"

type Type int

const (
	Prefix = "--gosql:"

	Default Type = iota
	ScanQuery
	ExplainQuery
	SchemeQuery

	DataQuery = Default
)

func (t Type) String() string {
	switch t {
	case DataQuery:
		return "DATA"
	case ScanQuery:
		return "SCAN"
	case ExplainQuery:
		return "EXPLAIN"
	case SchemeQuery:
		return "SCHEME"
	default:
		return ""
	}
}

func Mode(query string) Type {
	i := strings.Index(query, Prefix)
	if i < 0 {
		return DataQuery
	}
	query = strings.ToUpper(query[i+len(Prefix):])
	for _, m := range []Type{
		DataQuery,
		ScanQuery,
		SchemeQuery,
		ExplainQuery,
	} {
		if strings.HasPrefix(query, m.String()) {
			return m
		}
	}
	return DataQuery
}
