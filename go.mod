module github.com/ydb-platform/ydb-go-sql

go 1.16

require (
	github.com/golang/protobuf v1.5.0
	github.com/google/go-cmp v0.5.5
	github.com/stretchr/testify v1.5.1
	github.com/ydb-platform/ydb-go-genproto v0.0.0-20211103074319-526e57659e16
	github.com/ydb-platform/ydb-go-sdk/v3 v3.4.3
	google.golang.org/grpc v1.38.0
	google.golang.org/protobuf v1.26.0
)

replace github.com/ydb-platform/ydb-go-sdk/v3 => ../ydb-go-sdk-private
