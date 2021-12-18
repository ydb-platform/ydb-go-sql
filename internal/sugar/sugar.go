package sugar

import (
	"bytes"
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
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

func (d *Declaration) Declare(name string, t types.Type) {
	d.buf.WriteString("DECLARE $")
	d.buf.WriteString(name)
	d.buf.WriteString(" AS \"")
	types.WriteTypeStringTo(&d.buf, t)
	d.buf.WriteString("\";\n")
}
