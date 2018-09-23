package main

import (
	"fmt"
	"github.com/kirinrastogi/proxysql-go"
	"os"
)

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

func main() {
	conn, err := proxysql.New("remote-admin:password@tcp(localhost:6032)/", 0, 1)
	if err != nil {
		fmt.Print(err)
	}
	if err := conn.Ping(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println("connected successfully to ProxySQL")
	InsertWriter(conn)
	conn.RemoveHost("some-host")
}

func InsertWriter(conn ProxySQLConn) {
	conn.SetWriter("some-host", 3000)
}
