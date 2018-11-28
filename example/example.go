package main

// example application that runs along side ProxySQL

import (
	"github.com/kirinrastogi/proxysql-go"
	"log"
)

func main() {
	conn, err := proxysql.New("remote-admin:password@tcp(localhost:6032)/")
	if err != nil {
		log.Fatal(err)
	}

	err = conn.Clear()
	if err != nil {
		log.Fatal(err)
	}

	err = conn.AddHost(proxysql.Hostname("example"), proxysql.Hostgroup(1))
	if err != nil {
		log.Fatal(err)
	}

	err = conn.PersistChanges()
	if err != nil {
		log.Fatal(err)
	}

	entries, err := conn.All()
	if err != nil {
		log.Fatal(err)
	}

	for _, entry := range entries {
		log.Printf("entry: %v\n", entry)
	}
}
