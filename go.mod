module github.com/ydb-platform/ydb-go-sql

go 1.16

require (
	github.com/ydb-platform/ydb-go-sdk/v3 v3.4.5-0.20211219122617-b8ac46a7a856
	google.golang.org/grpc v1.38.0
	google.golang.org/protobuf v1.26.0
)

//replace (
//	github.com/ydb-platform/ydb-go-sdk/v3 => ../ydb-go-sdk-private
//)
