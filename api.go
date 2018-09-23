package proxysql

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

func NewWithDefaultHostgroups(dsn string) (*ProxySQL, error) {
	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return &ProxySQL{dsn: dsn, conn: conn, writerHostgroup: 0, readerHostgroup: 1}, nil
}

func New(dsn string, writerHostgroup int, readerHostgroup int) (*ProxySQL, error) {
	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return &ProxySQL{dsn, conn, writerHostgroup, readerHostgroup}, nil
}
