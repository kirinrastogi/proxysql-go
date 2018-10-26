package proxysql

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

type sqlOpen func(string, string) (*sql.DB, error)

var open sqlOpen

func init() {
	open = sql.Open
}

func New(dsn string) (*ProxySQL, error) {
	conn, err := open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return &ProxySQL{
		dsn:  dsn,
		conn: conn,
	}, nil
}
