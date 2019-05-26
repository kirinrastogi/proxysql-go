package main

// example application that runs along side ProxySQL

import (
	. "github.com/kirinrastogi/proxysql-go" // import into current namespace, use proxysql. if you omit this period
	"log"
)

type ProxySQLConn interface {
	AddHost(...HostOpts) error
	AddHosts(...*Host) error
	PersistChanges() error
}

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

	addHostsAndSave(conn)

	entries, err := conn.All()
	if err != nil {
		log.Fatal(err)
	}

	for _, entry := range entries {
		log.Printf("entry: %v\n", entry)
	}

	log.Println("Success")
}

func addHostsAndSave(conn ProxySQLConn) error {
	err := conn.AddHost(Hostname("example"), HostgroupID(1))
	if err != nil {
		return err
	}

	err = conn.AddHosts(DefaultHost().SetHostname("example2"))
	if err != nil {
		return err
	}

	err = conn.PersistChanges()
	if err != nil {
		return err
	}
	return nil
}
