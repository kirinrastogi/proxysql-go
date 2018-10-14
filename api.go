package proxysql

import (
	"database/sql"
	"errors"
	_ "github.com/go-sql-driver/mysql"
)

var open func(string, string) (*sql.DB, error) = sql.Open
var (
	ErrConfigBadTable error = errors.New("Bad default table, must be one of 'mysql_servers', 'runtime_mysql_servers'")
)

type Options struct {
	defaultTable string
}

type Opt func(*Options)

func DefaultTable(t string) Opt {
	return func(opt *Options) {
		opt.defaultTable = t
	}
}

func New(dsn string, setters ...Opt) (*ProxySQL, error) {
	opts := &Options{
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
		dsn:   dsn,
		conn:  conn,
		table: opts.defaultTable,
	}, nil
}
