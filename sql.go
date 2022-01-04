package ydb

import (
	"database/sql"

	"github.com/ydb-platform/ydb-go-sql/internal/driver"
)

var legacyDriver = driver.New()

func init() {
	sql.Register("ydb", legacyDriver)
}
