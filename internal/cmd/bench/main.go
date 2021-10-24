package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/ydb-platform/ydb-go-sql"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	db, err := sql.Open("ydb", os.Getenv("YDB_CONNECTION_STRING"))
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()
	if err = db.PingContext(ctx); err != nil {
		panic(err)
	}

	for i := 1; i < 100; i++ {
		rows, err := db.QueryContext(ctx, "SELECT 666")
		if err != nil {
			panic(err)
		}
		var res int
		for rows.NextResultSet() {
			for rows.Next() {
				if err = rows.Scan(&res); err != nil {
					panic(err)
				}
				fmt.Println(res)
			}
		}
	}
}
