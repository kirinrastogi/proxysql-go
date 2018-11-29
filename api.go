package proxysql

// this file is to show the entrypoint

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

type sqlOpen func(string, string) (*sql.DB, error)

var open sqlOpen

func init() {
	open = sql.Open
}

// NewProxySQL will create & return a pointer to a ProxySQL struct
// It will fail and return an error if the call to `sql.Open` fails
// This will really only fail if there is no memory left to create a connection struct
func NewProxySQL(dsn string) (*ProxySQL, error) {
	conn, err := open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return &ProxySQL{
		dsn:  dsn,
		conn: conn,
	}, nil
}
