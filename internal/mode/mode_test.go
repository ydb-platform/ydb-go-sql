package mode

import "testing"

func TestCheckQuery(t *testing.T) {
	type f func(string) bool
	for _, test := range []struct {
		name  string
		query string
		mode  Type
	}{
		{
			name: "data query",
			query: `
				--ydb:DATA 
				SELECT 1;
			`,
			mode: DataQuery,
		},
		{
			name: "data query without Prefix",
			query: `
				SELECT 1;
			`,
			mode: DataQuery,
		},
		{
			name: "scan query",
			query: `
				--ydb:SCAN 
				SELECT 1;
			`,
			mode: ScanQuery,
		},
		{
			name: "scheme query",
			query: `
				--ydb:SCHEME 
				CREATE TABLE t();
			`,
			mode: SchemeQuery,
		},
		{
			name: "explain",
			query: `
				--ydb:EXPLAIN 
				SELECT 1;
			`,
			mode: ExplainQuery,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if Mode(test.query) != test.mode {
				t.Fatalf("query '%s' is not a '%s' query", test.query, test.mode)
			}
		})
	}
}
