package main

// example application that runs along side ProxySQL

import (
	. "github.com/kirinrastogi/proxysql-go" // import into current namespace, use proxysql. if you omit this period
	"log"
)

func main() {
	// this is the dsn of the container that ./run.sh creates
	conn, err := NewProxySQL("remote-admin:password@tcp(localhost:6032)/")
	if err != nil {
		log.Fatal(err)
	}

	err = conn.Clear()
	if err != nil {
		log.Fatal(err)
	}

	err = conn.AddHost(Hostname("example"), Hostgroup(1))
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
