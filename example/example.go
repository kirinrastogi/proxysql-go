package main

import (
	"fmt"
	"github.com/kirinrastogi/proxysql-go"
	"os"
)

// Define your own interface to use when you write code
// This example only uses SetWriter, so only that is included
type ProxySQLConn interface {
	SetWriter(string, int) error
}

func main() {
	// set up your connection
	conn, err := proxysql.New("remote-admin:password@tcp(localhost:6032)/", 0, 1)
	if err != nil {
		fmt.Print(err)
	}
	// ensure it is valid
	if err := conn.Ping(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	// do something
	fmt.Println("connected successfully to ProxySQL")
	InsertWriter(conn, "some-host")
	conn.RemoveHost("some-host")
}

func InsertWriter(conn ProxySQLConn, writer string) {
	conn.SetWriter(writer, 3000)
}
