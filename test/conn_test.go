package conn

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"github.com/ydb-platform/ydb-go-sql"
)

func openDB(ctx context.Context) (*sql.DB, error) {
	var (
		dtrace trace.Driver
		ctrace trace.Table
	)
	trace.Stub(&dtrace, func(name string, args ...interface{}) {
		log.Printf("[driver] %s: %+v", name, trace.ClearContext(args))
	})
	trace.Stub(&ctrace, func(name string, args ...interface{}) {
		log.Printf("[client] %s: %+v", name, trace.ClearContext(args))
	})

	db := sql.OpenDB(ydb.Connector(
		ydb.WithConnectionString(os.Getenv("YDB_CONNECTION_STRING")),
		ydb.WithAnonymousCredentials(),
		ydb.WithTraceDriver(dtrace),
		ydb.WithTraceTable(ctrace),
	))

	return db, db.PingContext(ctx)
}

func TestLegacyDriverOpen(t *testing.T) {
	db, err := sql.Open("ydb", os.Getenv("YDB_CONNECTION_STRING"))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
}

func TestQuery(t *testing.T) {
	c := ydb.Connector(
		ydb.With(
			config.WithGrpcOptions(
				grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) (err error) {
					switch m := testutil.Method(method).Code(); m {
					case testutil.TableCreateSession:
						r, ok := (reply).(*Ydb_Table.CreateSessionResponse)
						if !ok {
							t.Fatalf("Unexpected response type %T", reply)
						}
						r.Operation.Result, err = anypb.New(
							&Ydb_Table.CreateSessionResult{
								SessionId: testutil.SessionID(),
							},
						)
						return err
					case testutil.TableExecuteDataQuery:
						{
							_, ok := (req).(*Ydb_Table.ExecuteDataQueryRequest)
							if !ok {
								t.Fatalf("Unexpected request type %T", req)
							}
						}
						{
							r, ok := (reply).(*Ydb_Table.ExecuteDataQueryResponse)
							if !ok {
								t.Fatalf("Unexpected response type %T", reply)
							}
							r.Operation.Result, err = anypb.New(
								&Ydb_Table.ExecuteQueryResult{
									TxMeta: &Ydb_Table.TransactionMeta{
										Id: "",
									},
								},
							)
							if err != nil {
								t.Fatalf("any proto failed: %v", err)
							}
							return nil
						}
					default:
						t.Fatalf("Unexpected method %d", m)
					}
					return nil
				}),
				grpc.WithChainStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
					switch m := testutil.Method(method).Code(); m {
					case testutil.TableStreamExecuteScanQuery:
						return nil, io.EOF
					default:
						t.Fatalf("Unexpected method %d", m)
					}
					return nil, fmt.Errorf("unexpected method %s", method)
				}),
			),
		),
		ydb.WithDefaultExecDataQueryOption(),
	)

	for _, test := range [...]struct {
		subName       string
		scanQueryMode bool
	}{
		{
			subName:       "Legacy",
			scanQueryMode: false,
		},
		{
			subName:       "WithScanQuery",
			scanQueryMode: true,
		},
	} {
		t.Run("QueryContext/Conn/"+test.subName, func(t *testing.T) {
			db := sql.OpenDB(c)
			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			defer cancel()
			if test.scanQueryMode {
				ctx = ydb.WithScanQuery(ctx)
			}
			rows, err := db.QueryContext(ctx, "SELECT 1")
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			if rows == nil {
				t.Fatal("query failed: nil rows")
			}
		})
		t.Run("QueryContext/STMT/"+test.subName, func(t *testing.T) {
			db := sql.OpenDB(c)
			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			defer cancel()
			stmt, err := db.PrepareContext(ctx, "SELECT 1")
			if err != nil {
				t.Fatalf("prepare failed: %v", err)
			}
			defer stmt.Close()
			if test.scanQueryMode {
				ctx = ydb.WithScanQuery(ctx)
			}
			rows, err := stmt.QueryContext(ctx)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			if rows == nil {
				t.Fatal("query failed: nil rows")
			}
		})
		t.Run("ExecContext/Conn/"+test.subName, func(t *testing.T) {
			db := sql.OpenDB(c)
			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			defer cancel()
			if test.scanQueryMode {
				ctx = ydb.WithScanQuery(ctx)
			}
			rows, err := db.ExecContext(ctx, "SELECT 1")
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			if rows == nil {
				t.Fatal("query failed: nil rows")
			}
		})
		t.Run("ExecContext/STMT/"+test.subName, func(t *testing.T) {
			db := sql.OpenDB(c)
			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			defer cancel()
			stmt, err := db.PrepareContext(ctx, "SELECT 1")
			if err != nil {
				t.Fatalf("prepare failed: %v", err)
			}
			defer stmt.Close()
			if test.scanQueryMode {
				ctx = ydb.WithScanQuery(ctx)
			}
			rows, err := stmt.ExecContext(ctx)
			if err != nil {
				t.Fatalf("stmt exec failed: %v", err)
			}
			if rows == nil {
				t.Fatal("stmt exec failed: nil rows")
			}
		})
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

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.PrepareContext(ctx, "SELECT NULL;")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	_, _ = stmt.Exec()
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

func TestDriver(t *testing.T) {
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

	rows, err := db.QueryContext(
		ctx, `
			DECLARE $seriesData AS List<Struct<
				series_id: Uint64,
				title: Utf8,
				series_info: Utf8,
				release_date: Date>>;
	
			SELECT
				series_id,
				title,
				series_info,
				release_date
			FROM AS_TABLE($seriesData);
		`,
		sql.Named("seriesData", getSeriesData()),
	)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var (
			seriesID    uint64
			title       string
			seriesInfo  string
			releaseDate time.Time
		)
		err := rows.Scan(
			&seriesID,
			&title,
			&seriesInfo,
			&releaseDate,
		)
		if err != nil {
			t.Fatal(err)
		}
		log.Printf("test: #%d %q %q %s", seriesID, title, seriesInfo, releaseDate)
	}
	log.Println("rows err", rows.Err())

	row := db.QueryRowContext(ctx, `
		DECLARE $dt AS Datetime;
		SELECT NULL, $dt;
	`,
		sql.Named("dt", time.Now()),
	)
	var (
		a *time.Time
		b time.Time
	)
	if err := row.Scan(&a, &b); err != nil {
		t.Fatal(err)
	}
	log.Println("date now:", a, b)
}

func getSeriesData() types.Value {
	return types.ListValue(
		seriesData(1, days("2006-02-03"), "IT Crowd", ""+
			"The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by "+
			"Ash Atalla and starring Chris O'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry."),
		seriesData(2, days("2014-04-06"), "Silicon Valley", ""+
			"Silicon Valley is an American comedy television series created by Mike Judge, John Altschuler and "+
			"Dave Krinsky. The series focuses on five young men who founded a startup company in Silicon Valley."),
	)
}

func seriesData(id uint64, released time.Time, title, info string) types.Value {
	return types.StructValue(
		types.StructFieldValue("series_id", types.Uint64Value(id)),
		types.StructFieldValue("release_date", types.DateValueFromTime(released)),
		types.StructFieldValue("title", types.UTF8Value(title)),
		types.StructFieldValue("series_info", types.UTF8Value(info)),
	)
}

func days(date string) time.Time {
	const ISO8601 = "2006-01-02"
	t, err := time.Parse(ISO8601, date)
	if err != nil {
		panic(err)
	}
	return t
}
