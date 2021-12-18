package conn

import (
	"database/sql/driver"
)

// Interface checks.
var (
	c conn

	_ driver.Conn               = &c
	_ driver.ExecerContext      = &c
	_ driver.QueryerContext     = &c
	_ driver.Pinger             = &c
	_ driver.SessionResetter    = &c
	_ driver.ConnPrepareContext = &c
	_ driver.ConnBeginTx        = &c
	_ driver.NamedValueChecker  = &c
)
