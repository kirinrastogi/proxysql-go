package proxysql

import (
	"database/sql"
	"errors"
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

func TestNewWithHostgroupsSetsHostgroups(t *testing.T) {
	conn, err := New("some-dsn", 33, 86)
	if err != nil {
		t.Fatal(err)
	}
	if conn.writerHostgroup != 33 || conn.readerHostgroup != 86 {
		t.Logf("hostgroups were not set properly: w: %d, r: %d", conn.writerHostgroup, conn.readerHostgroup)
		t.Fail()
	}
}

func TestNewWithDefaultHostgroupsSetsDefaultHostgroups(t *testing.T) {
	conn, err := NewWithDefaultHostgroups("some-dsn")
	if err != nil {
		t.Fatal(err)
	}
	if conn.writerHostgroup != 0 || conn.readerHostgroup != 1 {
		t.Log("default hostgroups were not 0 and 1")
		t.Fail()
	}
}

func TestNewErrorsOnSqlOpenError(t *testing.T) {
	open = func(driver string, dsn string) (*sql.DB, error) {
		return nil, errors.New("Error creating connection pool")
	}
	defer resetOpen()
	_, err := New("some-dsn", 0, 1)
	if err == nil {
		t.Log("New did not propogate err")
		t.Fail()
	}
}

func TestNewWithDefaultHostgroupsErrorsOnSqlOpenError(t *testing.T) {
	open = func(driver string, dsn string) (*sql.DB, error) {
		return nil, errors.New("Error creating connection pool")
	}
	defer resetOpen()
	_, err := NewWithDefaultHostgroups("some-dsn")
	if err == nil {
		t.Log("New did not propogate err")
		t.Fail()
	}
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
	if err := pool.Purge(proxysqlContainer); err != nil {
		t.Fatalf("could not purge proxysql: %v", err)
	}
}

func TestWriterErrorsIfThereIsNoWriter(t *testing.T) {
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
	writer, err := conn.Writer()
	if err == nil {
		t.Log("writer did not error when there were no rows")
		t.Fail()
	}
	if writer != "" {
		t.Log("writer hostname returned was non empty")
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

func TestHostExistsReturnsTrueForExistentHost(t *testing.T) {
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
	conn.Conn().Exec("insert into mysql_servers (hostgroup_id, hostname, max_connections) values (0, 'readerHost', 1000)")
	if exists, _ := conn.HostExists("readerHost"); !exists {
		t.Log("readerHost was inserted but not read")
		t.Fail()
	}
}

func TestHostExistsReturnsFalseForNonExistentHost(t *testing.T) {
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
	if exists, _ := conn.HostExists("readerHost"); exists {
		t.Log("readerHost was not inserted but read")
		t.Fail()
	}
}

func TestAllReturnsAllEntries(t *testing.T) {
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
	insertedEntries := map[string]int{
		"writer1": 0,
		"reader1": 1,
		"reader2": 1,
	}
	for hostname, hostgroup := range insertedEntries {
		query := fmt.Sprintf("insert into mysql_servers (hostgroup_id, hostname, max_connections) values (%d, '%s', 1000)", hostgroup, hostname)
		conn.Conn().Exec(query)
	}
	entries, err := conn.All()
	if err != nil {
		t.Logf("err while getting all entries")
		t.Fail()
	}
	for hostname, hostgroup := range insertedEntries {
		// if dne, fatalf
		// else, delete entry from entries
		returnedHostgroup, ok := entries[hostname]
		if !ok {
			t.Logf("entries did not contain key for %s. map: %v", hostname, entries)
			t.Fail()
		}
		if returnedHostgroup != hostgroup {
			t.Logf("hostgroup returned not equal to hostgroup inserted %d != %d for map %v", returnedHostgroup, hostgroup, entries)
			t.Fail()
		}
		delete(entries, hostname)
	}
}

func TestAllReturnsEmptyMapForEmptyTable(t *testing.T) {
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
	entries, err := conn.All()
	if err != nil {
		t.Logf("err while getting all entries")
		t.Fail()
	}
	if len(entries) != 0 {
		t.Logf("entries is nonzero for empty table: %v", entries)
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

func resetOpen() {
	open = sql.Open
}
