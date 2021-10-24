package ydbsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"regexp"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/resultset"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

var (
	ErrDeprecated          = errors.New("ydbsql: deprecated")
	ErrUnsupported         = errors.New("ydbsql: not supported")
	ErrActiveTransaction   = errors.New("ydbsql: can not begin tx within active tx")
	ErrNoActiveTransaction = errors.New("ydbsql: no active tx to work with")
)

func init() {
	sql.Register("ydb", new(legacyDriver))
}

type legacyDriver struct {
}

func (d *legacyDriver) OpenConnector(connection string) (driver.Connector, error) {
	return Connector(With(ydb.WithConnectionString(connection)))
}

func (d *legacyDriver) Open(name string) (driver.Conn, error) {
	return nil, ErrDeprecated
}

type ydbWrapper struct {
	dst *driver.Value
}

func (d *ydbWrapper) UnmarshalYDB(res types.RawValue) error {
	if res.IsOptional() {
		res.Unwrap()
	}
	if res.IsDecimal() {
		*d.dst = res.UnwrapDecimal()
	} else {
		*d.dst = res.Any()
	}
	return res.Err()
}

// sqlConn is a connection to the ydb.
type sqlConn struct {
	connector *sqlConnector // Immutable and r/o usage.
	client    table.Client

	txControl *table.TransactionControl
	dataOpts  []options.ExecuteDataQueryOption
	scanOpts  []options.ExecuteScanQueryOption

	idle bool

	tx  table.Transaction
	txc *table.TransactionControl
}

func (c *sqlConn) ResetSession(ctx context.Context) error {
	// TODO
	return nil
}

var namedValueParamNameRegex = regexp.MustCompile(`@(\w+)`)

func namedValueParamNames(q string, n int) ([]string, error) {
	var names []string
	matches := namedValueParamNameRegex.FindAllStringSubmatch(q, n)
	if m := len(matches); n != -1 && m < n {
		return nil, fmt.Errorf("query has %d placeholders but %d arguments are provided", m, n)
	}
	for _, m := range matches {
		names = append(names, m[1])
	}
	return names, nil
}

func (c *sqlConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	// TODO(jbd): Mention emails need to be escaped.
	args, err := namedValueParamNames(query, -1)
	if err != nil {
		return nil, err
	}
	return &stmt{conn: c, query: query, numArgs: len(args)}, nil
}

// txIsolationOrControl maps driver transaction options to ydb transaction option or query transaction control.
// This caused by ydb logic that prevents start actual transaction with OnlineReadOnly mode and ReadCommitted
// and ReadUncommitted isolation levels should use tx_control in every query request.
// It returns error on unsupported options.
func txIsolationOrControl(opts driver.TxOptions) (isolation table.TxOption, control []table.TxControlOption, err error) {
	level := sql.IsolationLevel(opts.Isolation)
	switch level {
	case sql.LevelDefault,
		sql.LevelSerializable,
		sql.LevelLinearizable:

		isolation = table.WithSerializableReadWrite()
		return

	case sql.LevelReadUncommitted:
		if opts.ReadOnly {
			control = []table.TxControlOption{
				table.BeginTx(
					table.WithOnlineReadOnly(
						table.WithInconsistentReads(),
					),
				),
				table.CommitTx(),
			}
			return
		}

	case sql.LevelReadCommitted:
		if opts.ReadOnly {
			control = []table.TxControlOption{
				table.BeginTx(
					table.WithOnlineReadOnly(),
				),
				table.CommitTx(),
			}
			return
		}
	}
	return nil, nil, fmt.Errorf(
		"unsupported transaction options: isolation=%s read_only=%t",
		nameIsolationLevel(level), opts.ReadOnly,
	)
}

func (c *sqlConn) BeginTx(ctx context.Context, opts driver.TxOptions) (tx driver.Tx, err error) {
	if c.tx != nil || c.txc != nil {
		return nil, ErrActiveTransaction
	}
	isolation, control, err := txIsolationOrControl(opts)
	if err != nil {
		return nil, err
	}
	if isolation != nil {
		err = c.client.Do(ctx, func(ctx context.Context, s table.Session) (err error) {
			c.tx, err = s.BeginTransaction(ctx, table.TxSettings(isolation))
			return err
		})
		if err != nil {
			return nil, err
		}
		c.txc = table.TxControl(table.WithTx(c.tx))
	} else {
		c.txc = table.TxControl(control...)
	}
	return c, nil
}

// Rollback implements driver.Tx interface.
// Note that it is called by driver even if a user did not called it.
func (c *sqlConn) Rollback() error {
	if c.tx == nil && c.txc == nil {
		return ErrNoActiveTransaction
	}

	tx := c.tx
	c.tx = nil
	c.txc = nil

	if tx != nil {
		return tx.Rollback(context.Background())
	}
	return nil
}

func (c *sqlConn) Commit() (err error) {
	if c.tx == nil && c.txc == nil {
		return ErrNoActiveTransaction
	}

	tx := c.tx
	c.tx = nil
	c.txc = nil

	if tx != nil {
		_, err = tx.CommitTx(context.Background())
	}
	return
}

func (c *sqlConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	_, err := c.exec(ctx, &reqQuery{text: query}, params(args))
	if err != nil {
		return nil, err
	}
	return result{}, nil
}

func (c *sqlConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if ContextScanQueryMode(ctx) {
		// Allow to use scanQuery only through QueryContext API.
		return c.scanQueryContext(ctx, query, args)
	}
	return c.queryContext(ctx, query, args)
}

func (c *sqlConn) queryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	res, err := c.exec(ctx, &reqQuery{text: query}, params(args))
	if err != nil {
		return nil, err
	}
	res.NextResultSet(ctx)
	return &rows{res: res}, nil
}

func (c *sqlConn) scanQueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	res, err := c.exec(ctx, &reqScanQuery{text: query}, params(args))
	if err != nil {
		return nil, err
	}
	res.NextResultSet(ctx)
	return &stream{ctx: ctx, res: res}, res.Err()
}

func (c *sqlConn) CheckNamedValue(v *driver.NamedValue) error {
	return checkNamedValue(v)
}

func (c *sqlConn) Ping(ctx context.Context) error {
	return c.client.Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.KeepAlive(ctx)
		},
	)
}

func (c *sqlConn) Close() error {
	return c.client.Close(context.Background())
}

func (c *sqlConn) Prepare(string) (driver.Stmt, error) {
	return nil, ErrDeprecated
}

func (c *sqlConn) Begin() (driver.Tx, error) {
	return nil, ErrDeprecated
}

func (c *sqlConn) exec(ctx context.Context, req processor, params *table.QueryParameters) (res resultset.Result, err error) {
	err = c.client.Do(
		ctx,
		func(ctx context.Context, session table.Session) (err error) {
			res, err = req.process(ctx, c, params)
			return err
		},
	)
	return res, err
}

type processor interface {
	process(context.Context, *sqlConn, *table.QueryParameters) (resultset.Result, error)
}

type reqStmt struct {
	stmt table.Statement
}

func (o *reqStmt) process(ctx context.Context, c *sqlConn, params *table.QueryParameters) (resultset.Result, error) {
	_, res, err := o.stmt.Execute(ctx, c.txControl, params, c.dataOpts...)
	return res, err
}

type reqQuery struct {
	text string
}

func (o *reqQuery) process(ctx context.Context, c *sqlConn, params *table.QueryParameters) (res resultset.Result, err error) {
	err = c.client.Do(ctx, func(ctx context.Context, s table.Session) (err error) {
		_, res, err = s.Execute(ctx, c.txControl, o.text, params, c.dataOpts...)
		return err
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

type reqScanQuery struct {
	text string
}

func (o *reqScanQuery) process(ctx context.Context, c *sqlConn, params *table.QueryParameters) (res resultset.Result, err error) {
	err = c.client.Do(ctx, func(ctx context.Context, s table.Session) (err error) {
		res, err = s.StreamExecuteScanQuery(ctx, o.text, params, c.scanOpts...)
		return err
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

type TxOperationFunc func(context.Context, *sql.Tx) error

// TxDoer contains options for retrying transactions.
type TxDoer struct {
	DB      *sql.DB
	Options *sql.TxOptions
}

// Do starts a transaction and calls f with it. If f() call returns a retryable
// error, it repeats it accordingly to retry configuration that TxDoer's DB
// driver holds.
//
// Note that callers should mutate state outside of f carefully and keeping in
// mind that f could be called again even if no error returned – transaction
// commitment can be failed:
//
//   var results []int
//   ydbsql.DoTx(ctx, db, TxOperationFunc(func(ctx deadline.Context, tx *sql.Tx) error {
//       // Reset resulting slice to prevent duplicates when retry occurred.
//       results = results[:0]
//
//       rows, err := tx.QueryContext(...)
//       if err != nil {
//           // handle error
//       }
//       for rows.Next() {
//           results = append(results, ...)
//       }
//       return rows.err()
//   }))
func (d TxDoer) Do(ctx context.Context, f TxOperationFunc) (err error) {
	err = retry.Retry(
		ctx,
		retry.IsOperationIdempotent(ctx),
		func(ctx context.Context) (err error) {
			return d.do(ctx, f)
		},
	)
	return err
}

func (d TxDoer) do(ctx context.Context, f TxOperationFunc) error {
	tx, err := d.DB.BeginTx(ctx, d.Options)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err := f(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

// DoTx is a shortcut for calling Do(ctx, f) on initialized TxDoer with DB
// field set to given db.
func DoTx(ctx context.Context, db *sql.DB, f TxOperationFunc) error {
	return (TxDoer{DB: db}).Do(ctx, f)
}

var isolationLevelName = [...]string{
	sql.LevelDefault:         "default",
	sql.LevelReadUncommitted: "read_uncommitted",
	sql.LevelReadCommitted:   "read_committed",
	sql.LevelWriteCommitted:  "write_committed",
	sql.LevelRepeatableRead:  "repeatable_read",
	sql.LevelSnapshot:        "snapshot",
	sql.LevelSerializable:    "serializable",
	sql.LevelLinearizable:    "linearizable",
}

func nameIsolationLevel(x sql.IsolationLevel) string {
	if int(x) < len(isolationLevelName) {
		return isolationLevelName[x]
	}
	return "unknown_isolation"
}

type stmt struct {
	conn    *sqlConn
	numArgs int
	query   string
}

func (s *stmt) NumInput() int {
	return s.numArgs
}

func (s *stmt) Close() error {
	return nil
}

func (s stmt) Exec([]driver.Value) (driver.Result, error) {
	return nil, ErrDeprecated
}

func (s stmt) Query([]driver.Value) (driver.Rows, error) {
	return nil, ErrDeprecated
}

func (s *stmt) CheckNamedValue(v *driver.NamedValue) error {
	return checkNamedValue(v)
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (_ driver.Result, err error) {
	return s.conn.ExecContext(ctx, s.query, args)
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if ContextScanQueryMode(ctx) {
		// Allow to use scanQuery only through QueryContext API.
		return s.scanQueryContext(ctx, args)
	}
	return s.queryContext(ctx, args)
}

func (s *stmt) queryContext(ctx context.Context, args []driver.NamedValue) (_ driver.Rows, err error) {
	var res resultset.Result
	err = s.conn.client.Do(
		ctx,
		func(ctx context.Context, session table.Session) (err error) {
			_, res, err = session.Execute(ctx, s.conn.txControl, s.query, params(args), s.conn.dataOpts...)
			return err
		},
	)
	if err != nil {
		return nil, err
	}
	_ = res.NextResultSet(ctx)
	return &rows{res: res}, nil
}

func (s *stmt) scanQueryContext(ctx context.Context, args []driver.NamedValue) (_ driver.Rows, err error) {
	var res resultset.Result
	err = s.conn.client.Do(
		ctx,
		func(ctx context.Context, session table.Session) (err error) {
			res, err = session.StreamExecuteScanQuery(ctx, s.query, params(args), s.conn.scanOpts...)
			return err
		},
	)
	if err != nil {
		return nil, err
	}
	_ = res.NextResultSet(ctx)
	return &stream{ctx: ctx, res: res}, res.Err()
}

func checkNamedValue(v *driver.NamedValue) (err error) {
	if v.Name == "" {
		return fmt.Errorf("ydbsql: only named parameters are supported")
	}

	if valuer, ok := v.Value.(driver.Valuer); ok {
		v.Value, err = valuer.Value()
		if err != nil {
			return fmt.Errorf("ydbsql: driver.Valuer error: %w", err)
		}
	}

	switch x := v.Value.(type) {
	case types.Value:
		// OK.
	case bool:
		v.Value = types.BoolValue(x)
	case int8:
		v.Value = types.Int8Value(x)
	case uint8:
		v.Value = types.Uint8Value(x)
	case int16:
		v.Value = types.Int16Value(x)
	case uint16:
		v.Value = types.Uint16Value(x)
	case int:
		v.Value = types.Int32Value(int32(x))
	case uint:
		v.Value = types.Uint32Value(uint32(x))
	case int32:
		v.Value = types.Int32Value(x)
	case uint32:
		v.Value = types.Uint32Value(x)
	case int64:
		v.Value = types.Int64Value(x)
	case uint64:
		v.Value = types.Uint64Value(x)
	case float32:
		v.Value = types.FloatValue(x)
	case float64:
		v.Value = types.DoubleValue(x)
	case []byte:
		v.Value = types.StringValue(x)
	case string:
		v.Value = types.UTF8Value(x)
	case [16]byte:
		v.Value = types.UUIDValue(x)

	default:
		return fmt.Errorf("ydbsql: unsupported types: %T", x)
	}

	v.Name = "$" + v.Name

	return nil
}

func params(args []driver.NamedValue) *table.QueryParameters {
	if len(args) == 0 {
		return nil
	}
	opts := make([]table.ParameterOption, len(args))
	for i, arg := range args {
		opts[i] = table.ValueParam(
			arg.Name,
			arg.Value.(types.Value),
		)
	}
	return table.NewQueryParameters(opts...)
}

type rows struct {
	res resultset.Result
}

func (r *rows) Columns() []string {
	var i int
	cs := make([]string, r.res.CurrentResultSet().ColumnCount())
	r.res.CurrentResultSet().Columns(func(m options.Column) {
		cs[i] = m.Name
		i++
	})
	return cs
}

func (r *rows) NextResultSet() error {
	if !r.res.NextResultSet(context.Background()) {
		return io.EOF
	}
	return nil
}

func (r *rows) HasNextResultSet() bool {
	return r.res.HasNextResultSet()
}

func (r *rows) Next(dst []driver.Value) error {
	if !r.res.NextRow() {
		return io.EOF
	}
	wraps := make([]interface{}, len(dst))
	for i := range dst {
		wraps[i] = &ydbWrapper{&dst[i]}
	}
	return r.res.Scan(wraps...)
}

func (r *rows) Close() error {
	return r.res.Close()
}

type stream struct {
	res resultset.Result
	ctx context.Context
}

func (r *stream) Columns() []string {
	var i int
	cs := make([]string, r.res.CurrentResultSet().ColumnCount())
	r.res.CurrentResultSet().Columns(func(m options.Column) {
		cs[i] = m.Name
		i++
	})
	return cs
}

func (r *stream) Next(dst []driver.Value) error {
	if !r.res.HasNextRow() {
		if !r.res.NextResultSet(r.ctx) {
			err := r.res.Err()
			if err != nil {
				return err
			}
			return io.EOF
		}
	}
	if !r.res.NextRow() {
		return io.EOF
	}
	for i := range dst {
		// NOTE: for queries like "SELECT * FROM xxx" order of columns is
		// undefined.
		_ = r.res.Scan(&ydbWrapper{&dst[i]})
	}
	return r.res.Err()
}

func (r *stream) Close() error {
	return r.res.Close()
}

type result struct{}

func (r result) LastInsertId() (int64, error) { return 0, ErrUnsupported }
func (r result) RowsAffected() (int64, error) { return 0, ErrUnsupported }
