package proxysql

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

var open func(string, string) (*sql.DB, error) = sql.Open

type options struct {
	defaultTable string
}

type opt func(*options)

func DefaultTable(t string) opt {
	return func(opts *options) {
		opts.defaultTable = t
	}
}

func New(dsn string, setters ...opt) (*ProxySQL, error) {
	opts := &options{
		defaultTable: "mysql_servers",
	}

	for _, setter := range setters {
		setter(opts)
	}

	// error check options set
	if opts.defaultTable != "mysql_servers" && opts.defaultTable != "runtime_mysql_servers" {
		return nil, ErrConfigBadTable
	}

	conn, err := open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return &ProxySQL{
		dsn:          dsn,
		conn:         conn,
		defaultTable: opts.defaultTable,
	}, nil
}
