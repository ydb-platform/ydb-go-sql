package driver

import (
	sql "database/sql/driver"
	"fmt"
	"net/url"

	"github.com/ydb-platform/ydb-go-sql/internal/connector"
	"github.com/ydb-platform/ydb-go-sql/internal/errors"
)

type Driver interface {
	sql.Driver
	sql.DriverContext
}

func New() Driver {
	return &driver{
		done: make(chan struct{}),
	}
}

// Driver is an adapter to allow the use table client as conn.Driver instance.
type driver struct {
	done chan struct{}
}

func (d *driver) Done() <-chan struct{} {
	return d.done
}

func (d *driver) Close() error {
	close(d.done)
	return nil
}

// Open returns a new connection to the ydb.
func (d *driver) Open(string) (sql.Conn, error) {
	return nil, errors.ErrDeprecated
}

func (d *driver) OpenConnector(uri string) (sql.Connector, error) {
	return connector.New(d, connector.WithConnectionString(uri)), nil
}

const (
	urlAuthToken = "auth-token"
)

func urlConnectorOptions(u *url.URL) []connector.Option {
	return []connector.Option{
		connector.WithEndpoint(u.Host),
		connector.WithDatabase(u.Path),
		connector.WithAccessTokenCredentials(u.Query().Get(urlAuthToken)),
	}
}

func validateURL(u *url.URL) error {
	if s := u.Scheme; s != "ydb" {
		return fmt.Errorf("malformed source uri: unexpected scheme: %q", s)
	}
	if u.Host == "" {
		return fmt.Errorf("malformed source uri: empty host")
	}
	if u.Path == "" {
		return fmt.Errorf("malformed source uri: empty database path")
	}

	var withToken bool
	for key := range u.Query() {
		if key != urlAuthToken {
			return fmt.Errorf("malformed source uri: unexpected option: %q", key)
		}
		withToken = true
	}
	if !withToken {
		return fmt.Errorf("malformed source uri: empty token")
	}

	return nil
}
