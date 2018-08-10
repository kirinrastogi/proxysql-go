package proxysql

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

func New(dsn string) (*ProxySQL, error) {
	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return &ProxySQL{dsn, conn}, nil
}

type ProxySQLConn interface {
	PersistChanges() error
	Ping() error
	Writer() (string, error)
	SetWriter(string, int) error
	HostExists(string) (bool, error)
	AddHost(string, int, int) error
	RemoveHost(string) error
	RemoveHostFromHostgroup(string, int) error
	All() (map[string]int, error)
	Hostgroup(int) (map[string]int, error)
	SizeOfHostgroup(int) (int, error)
}
