module github.com/ydb-platform/ydb-go-sql

go 1.16

require (
	github.com/ydb-platform/ydb-go-sdk/v3 v3.5.1
	google.golang.org/grpc v1.38.0
	google.golang.org/protobuf v1.26.0
)

//replace (
//	github.com/ydb-platform/ydb-go-sdk/v3 => ../ydb-go-sdk-private
//)
