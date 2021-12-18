# Go database/sql driver for YDB

This is an experimental version of Go database/sql driver for YDB. 
It is in active development and is not intended for use in production environments.

Package ydb provides integration of YDB table API with database/sql driver
interface.

## Usage

The recommended usage of this package is as follows:
```go
import "github.com/ydb-platform/ydb-go-sql"

func main() {
    db, err := sql.OpenDB(ydb.Connector(
        // Note that you could pass already configured instance of
        // ydb/table.Client here. This may be useful when tracing setup
        // must be prepared directly on Client structure.
        //
        // There are few other useful options, see ConnectorOption type for
        // more details.
    ))
}
```

But, as common for sql driver implementations, there is a standard way of
database initialization via sql.Open() function:

```go
import (
    "database/conn"
    _ "github.com/ydb-platform/ydb-go-sql" // for "ydb" conn driver registration.
)
func main() {
    db, err := sql.Open("ydb", "ydb://endpoint/database?auth-token=secret")
}
```

That is, data source name must be a welformed URL, with scheme "ydb", host for
YDB endpoint server, and path for database name. Note that token parameter is
required.

Data source name parameters:
token – access token to be used during requests (required).

As you may notice, initialization via sql.Open() does not provide ability to
setup tracing configuration.

Note that unlike ydb package, ydb retrying most ydb errors implicitly.
That is, calling db.QueryContext() or db.ExecContext() will retry operation
when it receives retriable error from ydb. But it is not possible to provide
same logic for transactions. There is a TxDoer struct or DoTx() function (which
is just a shortcut) for this purpose:

```go
import "github.com/ydb-platform/ydb-go-sql"

func main() {
    db, err := sql.OpenDB(ydb.Connector(...))
    if err != nil {
        // handle error
    }

    ctx, cancel := context.WithTimeout(10 * time.Second)
    defer cancel()

    err = db.ExecContext(ctx, ...) // Retries are implicit.
    if err != nil {
        // handle error
    }

    err = db.QueryContext(ctx, ...) // Retries are implicit.
    if err != nil {
        // handle error
    }

    // Explicit retries for transactions.
    err = ydb.DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
        // Execute statements here.
        // Note that Commit()/Rollback() are not neccessary here.
    })
    if err != nil {
        // handle error
    }
}
```

Note that database/sql package reuses sql.Conn instances which are wrappers
around ydb/table.Session instances in case of ydb. It could be reasonable to
increase the number of reused sessions via database/sql.DB.SetMaxIdleConns()
and database/sql.DB.SetMaxOpenConns() calls. If doing so, it is also highly
recommended to setup inner session pool's size limit to the same value by
passing WithSessionPoolSizeLimit() option to the Connector() function.

It is worth noting that YDB supports server side operation timeout. That is,
client could set up operation timeout among other operation options. When this
timeout exceeds, YDB will try to cancel operation execution and in any result
of the cancellation appropriate timeout error will be returned. By default, this
package "mirrors" context.Context deadlines and passes operation timeout option
if context deadline is not empty. To configure or disable such behavior please
see appropriate Connector options.


