package query

type Querier interface {
}

func New() Querier {
	return &querier{}
}

type querier struct {
}
