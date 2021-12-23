package ydb

import (
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

const Version = "ydb-go-sql/0.0.2"

const Versions = "[" + Version + "," + ydb.Version + ",grpc/" + grpc.Version + "]"
