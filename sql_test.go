package ydb

import (
	"context"
	"database/sql"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"github.com/ydb-platform/ydb-go-sql/internal/connector"
	"github.com/ydb-platform/ydb-go-sql/internal/stream"
	"log"
	"os"
	"testing"

	_ "github.com/ydb-platform/ydb-go-sql/internal/connector"
)

func TestLegacyDriverOpen(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testing in non-short mode")
	}
	db, err := sql.Open("ydb", os.Getenv("YDB_CONNECTION_STRING"))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
}

func TestDriverSelect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testing in non-short mode")
	}
	var (
		driverTrace trace.Driver
		tableTrace  trace.Table
	)
	trace.Stub(&driverTrace, func(name string, args ...interface{}) {
		log.Printf("[driver] %s: %+v", name, trace.ClearContext(args))
	})
	trace.Stub(&tableTrace, func(name string, args ...interface{}) {
		log.Printf("[table] %s: %+v", name, trace.ClearContext(args))
	})

	db := sql.OpenDB(stream.Result(
		connector.WithConnectionString(os.Getenv("YDB_CONNECTION_STRING")),
		connector.WithAnonymousCredentials(),
		connector.WithTraceDriver(driverTrace),
		connector.WithTraceTable(tableTrace),
	))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res, err := db.QueryContext(ctx, "SELECT 1+1")
	if err != nil {
		t.Fatal(err)
	}
	if !res.Next() {
		t.Fatal("nothing rows in set")
	}
	var v *int
	if err = res.Scan(&v); err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if v == nil {
		t.Fatal("nil value")
	}
	if *v != 2 {
		t.Fatalf("unexpected value: %v", *v)
	}
}
