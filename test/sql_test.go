//go:build !fast
// +build !fast

package conn

import (
	"context"
	"database/sql"
	"fmt"
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
			log.Printf("rows=%v", rows)
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

	_, _ = stmt.Exec()
	_, _ = stmt.Exec()

	_, _ = conn.QueryContext(ctx, "SELECT 42;")
}

func TestTx(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	db, err := openDB(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// ???????????????? ?????????????? ?????? ydb-???????? ??????????????????????
	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
		ReadOnly:  false,
	})
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()
	// ?? ???????? ?????????? ???????????????????? tx.Execute()
	res, err := tx.ExecContext(ctx, "INSERT INTO tbl (a, b) VALUES (1, 2);")
	if err != nil {
		panic(err)
	}
	lastInsertId, err := res.LastInsertId()
	if err != nil {
		// nop
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		// nop
	}
	fmt.Printf("insertedID=%d, rowsAffected=%d\n", lastInsertId, rowsAffected)
	// ?? ???????? ?????????? ???????????????????? tx.Execute()
	rows, err := tx.QueryContext(ctx, "SELECT * FROM tbl WHERE id=$id;", sql.Named("id", 1))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	_ = tx.Commit()

	time.Sleep(5 * time.Second)

	{
		rows, err := db.QueryContext(context.Background(), "SELECT 42")
		if err != nil {
			t.Fatal(err)
		}
		_ = rows.Close()

		time.Sleep(5 * time.Second)
	}
	time.Sleep(5 * time.Second)
}
