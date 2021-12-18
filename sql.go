package ydb

import (
	"database/sql"

	"github.com/ydb-platform/ydb-go-sql/internal/driver"
)

func init() {
	sql.Register("ydb", driver.New())
}
