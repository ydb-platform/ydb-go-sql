package ydb2sql

import "github.com/ydb-platform/ydb-go-sdk/v3/table/types"

type Valuer interface {
	Value() interface{}
}

type proxy struct {
	v interface{}
}

func (p *proxy) UnmarshalYDB(raw types.RawValue) error {
	p.v = raw.Any()
	return nil
}

func (p *proxy) Value() interface{} {
	return p.v
}

func New() Valuer {
	return &proxy{}
}
