package sugar

import (
	ydb_table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"strings"
	"testing"
)

func TestDeclaration(t *testing.T) {
	for _, test := range []struct {
		decl func(*Declaration)
		exp  string
	}{
		{
			decl: func(d *Declaration) {
				d.Declare("foo", ydb_table_types.TypeString)
				d.Declare("bar", ydb_table_types.TypeInt64)
				d.Declare("baz", ydb_table_types.Struct(
					ydb_table_types.StructField("foo", ydb_table_types.TypeString),
					ydb_table_types.StructField("bar", ydb_table_types.TypeInt64),
					ydb_table_types.StructField("baz", ydb_table_types.Tuple(
						ydb_table_types.TypeString, ydb_table_types.TypeInt64,
					)),
				))
			},
			exp: strings.Join([]string{
				"DECLARE $foo AS \"String\";",
				"DECLARE $bar AS \"Int64\";",
				"DECLARE $baz AS \"Struct<" +
					"foo:String," +
					"bar:Int64," +
					"baz:Tuple<String,Int64>>\";",
				"",
			}, "\n"),
		},
	} {
		t.Run("", func(t *testing.T) {
			var d Declaration
			test.decl(&d)
			if act, exp := d.String(), test.exp; act != exp {
				t.Fatalf("unexpected declaration: %q; want %q", act, exp)
			}
		})
	}
}
