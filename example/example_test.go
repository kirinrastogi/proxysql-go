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
	InsertWriter(mock)
	if w, _ := mock.Writer(); w != "some-host" {
		t.Fail()
	}
}

func TestInsertWriterInsertsWriterToContainer(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	defer SetupAndTeardownProxySQL()()
	// proxysql is up and running now
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := proxysql.New(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")), 0, 1)
	if err != nil {
		t.Log("bad dsn")
		t.Fail()
	}
	InsertWriter(conn)
	if writer, err := conn.Writer(); writer != "some-host" && err != nil {
		t.Log("writer not set")
		t.Fail()
	}
	conn.RemoveHost("some-host")
}

func SetupAndTeardownProxySQL() func() {
	log.Println("setting up proxysql container")
	var err error
	proxysqlContainer, err = pool.Run("kirinrastogi/proxysql", "latest", []string{})
	if err != nil {
		log.Fatalf("could not build and run proxysql, is dockerd running? error: %v", err)
	}
	log.Println("ran proxysql container, exponential backoff now")
	if err = pool.Retry(func() error {
		base := "remote-admin:password@tcp(localhost:%s)/"
		dsn := fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp"))
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			return err
		}
		log.Printf("pinging dsn: %s", dsn)
		return db.Ping()
	}); err != nil {
		log.Fatalf("could not connect to docker: %v", err)
	}
	return func() {
		if err = pool.Purge(proxysqlContainer); err != nil {
			log.Fatalf("could not purge proxysql: %v", err)
		}
		log.Println("purged a proxysql container")
	}
}
