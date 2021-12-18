package x

import (
	"database/sql/driver"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	ydb_table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func ToQueryParams(values []driver.NamedValue) *table.QueryParameters {
	if len(values) == 0 {
		return nil
	}
	opts := make([]table.ParameterOption, len(values))
	for i, arg := range values {
		opts[i] = table.ValueParam(
			arg.Name,
			arg.Value.(ydb_table_types.Value),
		)
	}
	return table.NewQueryParameters(opts...)
}

func ToSchemeOptions(values []driver.NamedValue) (opts []options.ExecuteSchemeQueryOption) {
	if len(values) == 0 {
		return nil
	}
	for _, arg := range values {
		opts = append(opts, arg.Value.(options.ExecuteSchemeQueryOption))
	}
	return opts
}
