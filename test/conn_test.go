package conn

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"testing"
	"text/template"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
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
	params, err := ydb.ConnectionString(os.Getenv("YDB_CONNECTION_STRING"))
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	db, err := sql.Open("ydb", params.String())
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()
	if err = db.PingContext(ctx); err != nil {
		panic(err)
	}
	if _, err = db.ExecContext(
		ydb.WithSchemeQuery(ctx), `
			CREATE TABLE series
			(
				series_id Uint64,
				comment Utf8,
				release_date Date,
				series_info Utf8,
				title Utf8,
				PRIMARY KEY (series_id)
			);
		`,
	); err != nil {
		panic(err)
	}
	if _, err = db.ExecContext(
		ydb.WithSchemeQuery(ctx), `
			CREATE TABLE seasons
			(
				series_id Uint64,
				season_id Uint64,
				first_aired Date,
				last_aired Date,
				title Utf8,
				PRIMARY KEY (series_id, season_id)
			);
		`,
	); err != nil {
		panic(err)
	}
	if _, err = db.ExecContext(
		ydb.WithSchemeQuery(ctx), `
			CREATE TABLE episodes
			(
				series_id Uint64,
				season_id Uint64,
				episode_id Uint64,
				air_date Date,
				title Utf8,
				PRIMARY KEY (series_id, season_id, episode_id)
			);
		`,
	); err != nil {
		panic(err)
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = tx.Rollback()
	}()
	row := tx.QueryRowContext(
		ydb.WithExplain(ctx),
		render(fill, templateConfig{
			TablePathPrefix: params.Database(),
		}),
		sql.Named("seriesData", getSeriesData()),
		sql.Named("seasonsData", getSeasonsData()),
		sql.Named("episodesData", getEpisodesData()),
	)
	if err != nil {
		panic(err)
	}
	var (
		ast  string
		plan string
	)
	if err = row.Scan(&ast, &plan); err != nil {
		panic(err)
	}
	log.Printf("> select_simple_transaction (explain):\n")
	log.Printf(
		"  > AST: %s\n",
		ast,
	)
	log.Printf(
		"  > Plan: %s\n",
		plan,
	)

	stmt, err := tx.PrepareContext(ctx, render(fill, templateConfig{
		TablePathPrefix: params.Database(),
	}))
	if err != nil {
		panic(err)
	}
	_, err = stmt.ExecContext(
		ydb.WithTxControl(ctx, table.TxControl(
			table.BeginTx(
				table.WithSerializableReadWrite(),
			),
			table.CommitTx(),
		)),
		sql.Named("seriesData", getSeriesData()),
		sql.Named("seasonsData", getSeasonsData()),
		sql.Named("episodesData", getEpisodesData()),
	)
	if err != nil {
		panic(err)
	}
	rows, err := tx.QueryContext(
		ctx,
		render(
			template.Must(template.New("").Parse(`
				PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
				DECLARE $seriesID AS Uint64;
	
				SELECT
					series_id,
					title,
					release_date
				FROM
					series
				WHERE
					series_id = $seriesID;
			`)),
			templateConfig{
				TablePathPrefix: params.Database(),
			},
		),
		sql.Named("seriesID", types.Uint64Value(1)),
	)
	if err != nil {
		panic(err)
	}
	if !rows.NextResultSet() {
		panic("no result sets")
	}
	if !rows.Next() {
		panic("no rows")
	}
	var (
		id    *uint64
		title *string
		date  *time.Time
	)
	if err = rows.Scan(&id, &title, &date); err != nil {
		panic(err)
	}
	log.Printf("> select_simple_transaction:\n")
	log.Printf(
		"  > %d %s %s\n",
		*id, *title, *date,
	)
	if err = tx.Commit(); err != nil {
		panic(err)
	}
}

func render(t *template.Template, data interface{}) string {
	var buf bytes.Buffer
	err := t.Execute(&buf, data)
	if err != nil {
		panic(err)
	}
	return buf.String()
}

type templateConfig struct {
	TablePathPrefix string
}

var fill = template.Must(template.New("fill database").Parse(`
	PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
	
	DECLARE $seriesData AS List<Struct<
		series_id: Uint64,
		title: Utf8,
		series_info: Utf8,
		release_date: Date,
		comment: Optional<Utf8>>>;
	
	DECLARE $seasonsData AS List<Struct<
		series_id: Uint64,
		season_id: Uint64,
		title: Utf8,
		first_aired: Date,
		last_aired: Date>>;
	
	DECLARE $episodesData AS List<Struct<
		series_id: Uint64,
		season_id: Uint64,
		episode_id: Uint64,
		title: Utf8,
		air_date: Date>>;
	
	REPLACE INTO series
	SELECT
		series_id,
		title,
		series_info,
		release_date,
		comment
	FROM AS_TABLE($seriesData);
	
	REPLACE INTO seasons
	SELECT
		series_id,
		season_id,
		title,
		first_aired,
		last_aired
	FROM AS_TABLE($seasonsData);
	
	REPLACE INTO episodes
	SELECT
		series_id,
		season_id,
		episode_id,
		title,
		air_date
	FROM AS_TABLE($episodesData);
`))

func seriesData(id uint64, released time.Time, title, info, comment string) types.Value {
	var commentv types.Value
	if comment == "" {
		commentv = types.NullValue(types.TypeUTF8)
	} else {
		commentv = types.OptionalValue(types.UTF8Value(comment))
	}
	return types.StructValue(
		types.StructFieldValue("series_id", types.Uint64Value(id)),
		types.StructFieldValue("release_date", types.DateValueFromTime(released)),
		types.StructFieldValue("title", types.UTF8Value(title)),
		types.StructFieldValue("series_info", types.UTF8Value(info)),
		types.StructFieldValue("comment", commentv),
	)
}

func seasonData(seriesID, seasonID uint64, title string, first, last time.Time) types.Value {
	return types.StructValue(
		types.StructFieldValue("series_id", types.Uint64Value(seriesID)),
		types.StructFieldValue("season_id", types.Uint64Value(seasonID)),
		types.StructFieldValue("title", types.UTF8Value(title)),
		types.StructFieldValue("first_aired", types.DateValueFromTime(first)),
		types.StructFieldValue("last_aired", types.DateValueFromTime(last)),
	)
}

func episodeData(seriesID, seasonID, episodeID uint64, title string, date time.Time) types.Value {
	return types.StructValue(
		types.StructFieldValue("series_id", types.Uint64Value(seriesID)),
		types.StructFieldValue("season_id", types.Uint64Value(seasonID)),
		types.StructFieldValue("episode_id", types.Uint64Value(episodeID)),
		types.StructFieldValue("title", types.UTF8Value(title)),
		types.StructFieldValue("air_date", types.DateValueFromTime(date)),
	)
}

func getSeriesData() types.Value {
	return types.ListValue(
		seriesData(
			1, days("2006-02-03"), "IT Crowd", ""+
				"The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by "+
				"Ash Atalla and starring Chris O'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry.",
			"", // NULL comment.
		),
		seriesData(
			2, days("2014-04-06"), "Silicon Valley", ""+
				"Silicon Valley is an American comedy television series created by Mike Judge, John Altschuler and "+
				"Dave Krinsky. The series focuses on five young men who founded a startup company in Silicon Valley.",
			"Some comment here",
		),
	)
}

func getSeasonsData() types.Value {
	return types.ListValue(
		seasonData(1, 1, "Season 1", days("2006-02-03"), days("2006-03-03")),
		seasonData(1, 2, "Season 2", days("2007-08-24"), days("2007-09-28")),
		seasonData(1, 3, "Season 3", days("2008-11-21"), days("2008-12-26")),
		seasonData(1, 4, "Season 4", days("2010-06-25"), days("2010-07-30")),
		seasonData(2, 1, "Season 1", days("2014-04-06"), days("2014-06-01")),
		seasonData(2, 2, "Season 2", days("2015-04-12"), days("2015-06-14")),
		seasonData(2, 3, "Season 3", days("2016-04-24"), days("2016-06-26")),
		seasonData(2, 4, "Season 4", days("2017-04-23"), days("2017-06-25")),
		seasonData(2, 5, "Season 5", days("2018-03-25"), days("2018-05-13")),
	)
}

func getEpisodesData() types.Value {
	return types.ListValue(
		episodeData(1, 1, 1, "Yesterday's Jam", days("2006-02-03")),
		episodeData(1, 1, 2, "Calamity Jen", days("2006-02-03")),
		episodeData(1, 1, 3, "Fifty-Fifty", days("2006-02-10")),
		episodeData(1, 1, 4, "The Red Door", days("2006-02-17")),
		episodeData(1, 1, 5, "The Haunting of Bill Crouse", days("2006-02-24")),
		episodeData(1, 1, 6, "Aunt Irma Visits", days("2006-03-03")),
		episodeData(1, 2, 1, "The Work Outing", days("2006-08-24")),
		episodeData(1, 2, 2, "Return of the Golden Child", days("2007-08-31")),
		episodeData(1, 2, 3, "Moss and the German", days("2007-09-07")),
		episodeData(1, 2, 4, "The Dinner Party", days("2007-09-14")),
		episodeData(1, 2, 5, "Smoke and Mirrors", days("2007-09-21")),
		episodeData(1, 2, 6, "Men Without Women", days("2007-09-28")),
		episodeData(1, 3, 1, "From Hell", days("2008-11-21")),
		episodeData(1, 3, 2, "Are We Not Men?", days("2008-11-28")),
		episodeData(1, 3, 3, "Tramps Like Us", days("2008-12-05")),
		episodeData(1, 3, 4, "The Speech", days("2008-12-12")),
		episodeData(1, 3, 5, "Friendface", days("2008-12-19")),
		episodeData(1, 3, 6, "Calendar Geeks", days("2008-12-26")),
		episodeData(1, 4, 1, "Jen The Fredo", days("2010-06-25")),
		episodeData(1, 4, 2, "The Final Countdown", days("2010-07-02")),
		episodeData(1, 4, 3, "Something Happened", days("2010-07-09")),
		episodeData(1, 4, 4, "Italian For Beginners", days("2010-07-16")),
		episodeData(1, 4, 5, "Bad Boys", days("2010-07-23")),
		episodeData(1, 4, 6, "Reynholm vs Reynholm", days("2010-07-30")),
		episodeData(2, 1, 1, "Minimum Viable Product", days("2014-04-06")),
		episodeData(2, 1, 2, "The Cap Table", days("2014-04-13")),
		episodeData(2, 1, 3, "Articles of Incorporation", days("2014-04-20")),
		episodeData(2, 1, 4, "Fiduciary Duties", days("2014-04-27")),
		episodeData(2, 1, 5, "Signaling Risk", days("2014-05-04")),
		episodeData(2, 1, 6, "Third Party Insourcing", days("2014-05-11")),
		episodeData(2, 1, 7, "Proof of Concept", days("2014-05-18")),
		episodeData(2, 1, 8, "Optimal Tip-to-Tip Efficiency", days("2014-06-01")),
		episodeData(2, 2, 1, "Sand Hill Shuffle", days("2015-04-12")),
		episodeData(2, 2, 2, "Runaway Devaluation", days("2015-04-19")),
		episodeData(2, 2, 3, "Bad Money", days("2015-04-26")),
		episodeData(2, 2, 4, "The Lady", days("2015-05-03")),
		episodeData(2, 2, 5, "Server Space", days("2015-05-10")),
		episodeData(2, 2, 6, "Homicide", days("2015-05-17")),
		episodeData(2, 2, 7, "Adult Content", days("2015-05-24")),
		episodeData(2, 2, 8, "White Hat/Black Hat", days("2015-05-31")),
		episodeData(2, 2, 9, "Binding Arbitration", days("2015-06-07")),
		episodeData(2, 2, 10, "Two Days of the Condor", days("2015-06-14")),
		episodeData(2, 3, 1, "Founder Friendly", days("2016-04-24")),
		episodeData(2, 3, 2, "Two in the Box", days("2016-05-01")),
		episodeData(2, 3, 3, "Meinertzhagen's Haversack", days("2016-05-08")),
		episodeData(2, 3, 4, "Maleant Data Systems Solutions", days("2016-05-15")),
		episodeData(2, 3, 5, "The Empty Chair", days("2016-05-22")),
		episodeData(2, 3, 6, "Bachmanity Insanity", days("2016-05-29")),
		episodeData(2, 3, 7, "To Build a Better Beta", days("2016-06-05")),
		episodeData(2, 3, 8, "Bachman's Earnings Over-Ride", days("2016-06-12")),
		episodeData(2, 3, 9, "Daily Active Users", days("2016-06-19")),
		episodeData(2, 3, 10, "The Uptick", days("2016-06-26")),
		episodeData(2, 4, 1, "Success Failure", days("2017-04-23")),
		episodeData(2, 4, 2, "Terms of Service", days("2017-04-30")),
		episodeData(2, 4, 3, "Intellectual Property", days("2017-05-07")),
		episodeData(2, 4, 4, "Teambuilding Exercise", days("2017-05-14")),
		episodeData(2, 4, 5, "The Blood Boy", days("2017-05-21")),
		episodeData(2, 4, 6, "Customer Service", days("2017-05-28")),
		episodeData(2, 4, 7, "The Patent Troll", days("2017-06-04")),
		episodeData(2, 4, 8, "The Keenan Vortex", days("2017-06-11")),
		episodeData(2, 4, 9, "Hooli-Con", days("2017-06-18")),
		episodeData(2, 4, 10, "Server Error", days("2017-06-25")),
		episodeData(2, 5, 1, "Grow Fast or Die Slow", days("2018-03-25")),
		episodeData(2, 5, 2, "Reorientation", days("2018-04-01")),
		episodeData(2, 5, 3, "Chief Operating Officer", days("2018-04-08")),
		episodeData(2, 5, 4, "Tech Evangelist", days("2018-04-15")),
		episodeData(2, 5, 5, "Facial Recognition", days("2018-04-22")),
		episodeData(2, 5, 6, "Artificial Emotional Intelligence", days("2018-04-29")),
		episodeData(2, 5, 7, "Initial Coin Offering", days("2018-05-06")),
		episodeData(2, 5, 8, "Fifty-One Percent", days("2018-05-13")),
	)
}

const DateISO8601 = "2006-01-02"

func days(date string) time.Time {
	t, err := time.Parse(DateISO8601, date)
	if err != nil {
		panic(err)
	}
	return t
}
