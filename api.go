package proxysql

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

func NewWithDefaults(dsn string) (*ProxySQL, error) {
	conn, err := open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return &ProxySQL{dsn: dsn, conn: conn, table: "mysql_servers", writerHostgroup: 0, readerHostgroup: 1}, nil
}

func New(dsn string, writerHostgroup int, readerHostgroup int, table string) (*ProxySQL, error) {
	conn, err := open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return &ProxySQL{dsn, conn, table, writerHostgroup, readerHostgroup}, nil
}

var open func(string, string) (*sql.DB, error) = sql.Open
