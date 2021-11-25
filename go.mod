module github.com/ydb-platform/ydb-go-sql

go 1.16

require (
	github.com/ydb-platform/ydb-go-genproto v0.0.0-20210916081217-f4e55570b874
	github.com/ydb-platform/ydb-go-sdk/v3 v3.0.1-rc0
	google.golang.org/protobuf v1.26.0
)

replace github.com/ydb-platform/ydb-go-sdk/v3 => ../ydb-go-sdk-private
