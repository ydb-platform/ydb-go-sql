package sugar

import (
	"bytes"
	ydb_table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"io"
)

type Declaration struct {
	buf bytes.Buffer
}

func (d *Declaration) WriteTo(dest io.Writer) (int64, error) {
	return d.buf.WriteTo(dest)
}

func (d *Declaration) String() string {
	return d.buf.String()
}

func (d *Declaration) Declare(name string, t ydb_table_types.Type) {
	d.buf.WriteString("DECLARE $")
	d.buf.WriteString(name)
	d.buf.WriteString(" AS \"")
	ydb_table_types.WriteTypeStringTo(&d.buf, t)
	d.buf.WriteString("\";\n")
}
