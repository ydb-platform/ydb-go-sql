//go:build !fast
// +build !fast

package conn

import (
	"context"
	"database/sql"
	"log"
	"os"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"github.com/ydb-platform/ydb-go-sql"
)

func TestLegacyDriverOpen(t *testing.T) {
	db, err := sql.Open("ydb", os.Getenv("YDB_CONNECTION_STRING"))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
}

func TestDriverSelect(t *testing.T) {
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

	db := sql.OpenDB(ydb.Connector(
		ydb.WithConnectionString(os.Getenv("YDB_CONNECTION_STRING")),
		ydb.WithAnonymousCredentials(),
		ydb.WithTraceDriver(driverTrace),
		ydb.WithTraceTable(tableTrace),
	))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res, err := db.QueryContext(ctx, "SELECT 1+1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err = res.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	if !res.NextResultSet() {
		t.Fatal("nothing result sets")
	}
	if !res.Next() {
		t.Fatal("nothing rows in set")
	}
	var v *int
	if err = res.Scan(&v); err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if err = res.Err(); err != nil {
		t.Fatal(err)
	}
	if v == nil {
		t.Fatal("nil value")
	}
	if *v != 2 {
		t.Fatalf("unexpected value: %v", *v)
	}
}

func TestDatabaseSelect(t *testing.T) {
	for _, test := range []struct {
		query  string
		params []interface{}
	}{
		{
			query: "DECLARE $a AS INT64; SELECT $a",
			params: []interface{}{
				sql.Named("a", int64(1)),
			},
		},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		t.Run("exec", func(t *testing.T) {
			db, err := openDB(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			res, err := db.ExecContext(ctx, test.query, test.params...)
			if err != nil {
				t.Fatal(err)
			}
			log.Printf("result=%v", res)
		})
		t.Run("query", func(t *testing.T) {
			db, err := openDB(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			rows, err := db.QueryContext(ctx, test.query, test.params...)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err = rows.Close(); err != nil {
					t.Fatal(err)
				}
			}()
			log.Printf("rows=%v", rows)
			if err = rows.Err(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStatement(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testing in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	db, err := openDB(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := conn.PrepareContext(ctx, "SELECT NULL;")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	// nolint: godox
	// TODO: other queries
}

func TestTx(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	db, err := openDB(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = db.Close()
	}()

	// create test table
	_, err = db.ExecContext(ctx,
		ydb.SchemeQuery(`CREATE TABLE test_tx (a Uint64, b Utf8, PRIMARY KEY (a, b))`),
	)
	if err != nil {
		t.Fatal(err)
	}

	// make tx
	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
		ReadOnly:  false,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = tx.Rollback()
	}()
	// must call tx.Execute
	_, err = tx.ExecContext(ctx,
		ydb.DataQuery(`UPSERT INTO test_tx (a, b) VALUES (1, "2");`),
	)
	if err != nil {
		t.Fatal(err)
	}
	_ = tx.Commit()
	// must call session.Execute
	rows, err := db.QueryContext(
		ctx,
		`
			DECLARE $a AS Uint64;
			SELECT * FROM test_tx WHERE a=$a;
		`,
		sql.Named("a", ydb.Uint64(1)),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
}
