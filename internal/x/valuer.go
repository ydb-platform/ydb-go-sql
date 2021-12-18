package x

import "github.com/ydb-platform/ydb-go-sdk/v3/table/types"

// Valuer based on types.Scanner interface and provides access to value
type Valuer interface {
	types.Scanner

	// Value returns internal value
	Value() interface{}
}

type valuer struct {
	v interface{}
}

func (p *valuer) UnmarshalYDB(raw types.RawValue) error {
	p.v = raw.Any()
	return nil
}

func (p *valuer) Value() interface{} {
	return p.v
}

// V makes Valuer
func V() Valuer {
	return &valuer{}
}
