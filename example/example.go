package main

import (
	"fmt"
	"github.com/kirinrastogi/proxysql-go"
	"os"
)

func main() {
	conn, err := proxysql.New("remote-admin:password@tcp(localhost:6032)/")
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

func InsertWriter(conn proxysql.ProxySQLConn) {
	conn.SetWriter("some-host", 3000)
}
