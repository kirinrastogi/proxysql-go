package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/kirinrastogi/proxysql-go"
	"github.com/kirinrastogi/proxysql-go/mocks"
	"github.com/ory/dockertest"
	"log"
	"os"
	"testing"
)

var pool *dockertest.Pool
var proxysqlContainer *dockertest.Resource

func TestMain(m *testing.M) {
	var err error
	pool, err = dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not reach docker daemon: %v", err)
	}
	code := m.Run()
	os.Exit(code)
}

func TestInsertWriterInsertsWriter(t *testing.T) {
	mock := proxysql_mock.New()
	writerHostname := "a-host"
	InsertWriter(mock, writerHostname)
	if writer, _ := mock.Writer(); writer != writerHostname {
		t.Fail()
	}
}

func TestInsertWriterInsertsWriterToContainer(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	defer SetupAndTeardownProxySQL(t)()
	// proxysql is up and running now
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := proxysql.New(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")), 0, 1, "mysql_servers")
	if err != nil {
		t.Log("bad dsn")
		t.Fail()
	}
	writerHostname := "b-host"
	InsertWriter(conn, writerHostname)
	if writer, err := conn.Writer(); writer != writerHostname && err != nil {
		t.Log("writer not set")
		t.Fail()
	}
	conn.RemoveHost("some-host")
}

func SetupAndTeardownProxySQL(t *testing.T) func() {
	t.Log("setting up proxysql container")
	var err error
	proxysqlContainer, err = pool.Run("kirinrastogi/proxysql", "latest", []string{})
	if err != nil {
		t.Fatalf("could not build and run proxysql, is dockerd running? error: %v", err)
	}
	t.Log("ran proxysql container, waiting for network connection")
	if err = pool.Retry(func() error {
		base := "remote-admin:password@tcp(localhost:%s)/"
		dsn := fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp"))
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			return err
		}
		t.Logf("pinging dsn: %s", dsn)
		return db.Ping()
	}); err != nil {
		t.Fatalf("could not connect to docker: %v", err)
	}
	return func() {
		if err = pool.Purge(proxysqlContainer); err != nil {
			t.Fatalf("could not purge proxysql: %v", err)
		}
		t.Log("purged a proxysql container")
	}
}
