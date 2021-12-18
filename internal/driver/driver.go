package driver

import (
	"database/sql/driver"

	"github.com/ydb-platform/ydb-go-sql/internal/connector"
	"github.com/ydb-platform/ydb-go-sql/internal/errors"
)

type Driver interface {
	driver.Driver
	driver.DriverContext
	connector.Driver
}

func New() Driver {
	return &legacyDriver{
		done: make(chan struct{}),
	}
}

// Driver is an adapter to allow the use table client as conn.Driver instance.
type legacyDriver struct {
	done chan struct{}
}

func (d *legacyDriver) Done() <-chan struct{} {
	return d.done
}

func (d *legacyDriver) Close() error {
	close(d.done)
	return nil
}

// Open returns a new connection to the ydb.
func (d *legacyDriver) Open(string) (driver.Conn, error) {
	return nil, errors.ErrDeprecated
}

func (d *legacyDriver) OpenConnector(uri string) (driver.Connector, error) {
	return connector.New(d, connector.WithConnectionString(uri)), nil
}
