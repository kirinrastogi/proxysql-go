package proxysql

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
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

func TestPingSucceedsOnLiveContainer(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	defer SetupAndTeardownProxySQL(t)()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := New(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")), 0, 1)
	if err != nil {
		t.Log("bad dsn")
		t.Fail()
	}
	if err := conn.Ping(); err != nil {
		t.Logf("ping failed to live container at %s", base)
		t.Fail()
	}
}

func TestPingFailsOnDeadContainer(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	SetupProxySQL(t)
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := New(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")), 0, 1)
	if err != nil {
		t.Log("bad dsn")
		t.Fail()
	}
	if err := pool.Purge(proxysqlContainer); err != nil {
		t.Fatalf("could not purge proxysql: %v", err)
	}
	if err := conn.Ping(); err == nil {
		t.Logf("ping succeeded to live container at %s", base)
		t.Fail()
	}
}

func TestCloseClosesConnectionToProxySQL(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	SetupProxySQL(t)
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := New(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")), 0, 1)
	if err != nil {
		t.Log("bad dsn")
		t.Fail()
	}
	conn.Close()
	if err := conn.Ping(); err == nil {
		t.Logf("ping succeeded to container with closed connection %s", base)
		t.Fail()
	}
}

func TestWriterReadsTheWriter(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	defer SetupAndTeardownProxySQL(t)()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := New(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")), 0, 1)
	if err != nil {
		t.Log("bad dsn")
		t.Fail()
	}
	t.Log("inserting into ProxySQL")
	conn.Conn().Exec("insert into mysql_servers (hostgroup_id, hostname, max_connections) values (0, 'writerHost', 1000)")
	conn.Conn().Exec("insert into mysql_servers (hostgroup_id, hostname, max_connections) values (1, 'readerHost', 1000)")
	writer, err := conn.Writer()
	if err != nil {
		t.Fatalf("could not get writer: %v", err)
	}
	if writer != "writerHost" {
		t.Log("writer set was not the writer read")
		t.Fail()
	}
}

func TestSetWriterSetsTheWriter(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	defer SetupAndTeardownProxySQL(t)()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := New(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")), 0, 1)
	if err != nil {
		t.Log("bad dsn")
		t.Fail()
	}
	t.Log("inserting into ProxySQL")
	writerHostname := "some-writer"
	err = conn.SetWriter(writerHostname, 1000)
	writer, err := conn.Writer()
	if err != nil {
		t.Fatalf("could not get writer: %v", err)
	}
	if writer != writerHostname {
		t.Log("writer set was not the writer read")
		t.Fail()
	}
}

func SetupAndTeardownProxySQL(t *testing.T) func() {
	SetupProxySQL(t)
	return func() {
		if err := pool.Purge(proxysqlContainer); err != nil {
			t.Fatalf("could not purge proxysql: %v", err)
		}
		t.Log("purged a proxysql container")
	}
}

func SetupProxySQL(t *testing.T) {
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
}
