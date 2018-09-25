package proxysql

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/ory/dockertest"
	"log"
	"os"
	"strings"
	"testing"
	"time"
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
	defer SetupAndTeardownProxySQL(t)()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := New(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")), 0, 1)
	if err != nil {
		t.Fatal("bad dsn")
	}
	if err := conn.Ping(); err != nil {
		t.Logf("ping failed to live container at %s", base)
		t.Fail()
	}
}

func TestPingFailsOnDeadContainer(t *testing.T) {
	SetupProxySQL(t)
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := New(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")), 0, 1)
	if err != nil {
		t.Fatal("bad dsn")
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
	SetupProxySQL(t)
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := New(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")), 0, 1)
	if err != nil {
		t.Fatal("bad dsn")
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
	defer SetupAndTeardownProxySQL(t)()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := New(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")), 0, 1)
	if err != nil {
		t.Fatal("bad dsn")
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
	defer SetupAndTeardownProxySQL(t)()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := New(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")), 0, 1)
	if err != nil {
		t.Fatal("bad dsn")
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
	defer SetupAndTeardownProxySQL(t)()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := New(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")), 0, 1)
	if err != nil {
		t.Fatal("bad dsn")
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

func TestSetWriterUpdatesExistingWriter(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := New(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")), 0, 1)
	if err != nil {
		t.Fatal("bad dsn")
	}
	t.Log("inserting into ProxySQL")
	oldWriterHostname := "old-writer"
	err = conn.SetWriter(oldWriterHostname, 1000)
	if err != nil {
		t.Logf("inserting old writer failed %v", err)
		t.Fail()
	}
	writerHostname := "some-writer"
	err = conn.SetWriter(writerHostname, 1000)
	if err != nil {
		t.Fatalf("inserting new writer failed %v", err)
	}
	writer, err := conn.Writer()
	if err != nil {
		t.Fatalf("could not get writer: %v", err)
	}
	if writer != writerHostname {
		t.Logf("writer set was not the writer read, %s != %s", writer, writerHostname)
		t.Fail()
	}
}

func TestSetWriterInsertsOnErrNoRows(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	defer resetExec()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := New(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")), 0, 1)
	if err != nil {
		t.Fatal("bad dsn")
	}

	queryType := ""
	fullQuery := ""
	// when SetWriter execs, make it return the first word in query
	exec = func(p *ProxySQL, query string, args ...interface{}) (sql.Result, error) {
		queryType = strings.Split(query, " ")[0]
		fullQuery = query
		return p.conn.Exec(query)
	}
	writerSet := "new-writer"
	if err := conn.SetWriter(writerSet, 2000); err != nil {
		t.Fatalf("error inserting writer: %v", err)
	}
	// assert queryType is "insert"
	// assert Writer() is "new-writer"
	writer, err := conn.Writer()
	if err != nil {
		t.Fatalf("error getting writer %v", err)
	}
	if writer != writerSet {
		t.Logf("got writer different from set writer %s != %s", writer, writerSet)
		t.Fail()
	}

	if queryType != "insert" {
		t.Logf("SetWriter did not insert to ProxySQL, instead it ran: \n%s", fullQuery)
		t.Fail()
	}
}

func TestSetWriterErrorsOnInsertionError(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	defer resetExec()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := New(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")), 0, 1)
	if err != nil {
		t.Fatal("bad dsn")
	}
	exec = func(p *ProxySQL, query string, args ...interface{}) (sql.Result, error) {
		return nil, errors.New("could not insert")
	}
	writerSet := "new-writer"
	err = conn.SetWriter(writerSet, 2000)
	if err == nil {
		t.Log("SetWriter did not error on exec insertion error")
		t.Fail()
	}
}

func TestSetWriterErrorsOnUpdateError(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	defer resetExec()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := New(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")), 0, 1)
	if err != nil {
		t.Fatal("bad dsn")
	}

	queryType := ""
	fullQuery := ""
	// when SetWriter execs to update, make it return the first word in query
	exec = func(p *ProxySQL, query string, args ...interface{}) (sql.Result, error) {
		queryType = strings.Split(query, " ")[0]
		fullQuery = query
		return nil, errors.New("could not update writer")
	}
	oldWriter := "old-writer"
	setupQuery := fmt.Sprintf("insert into mysql_servers (hostgroup_id, hostname, max_connections) values (0, '%s', 1000)", oldWriter)
	_, err = conn.conn.Exec(setupQuery)
	if err != nil {
		t.Fatalf("err setting writer in setup %s, err: %v", oldWriter, err)
	}
	writerSet := "new-writer"
	if err := conn.SetWriter(writerSet, 2000); err == nil {
		t.Log("setwriter did not error on error updating writer")
		t.Fail()
	}
}

func TestHostExistsReturnsTrueForExistentHost(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := New(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")), 0, 1)
	if err != nil {
		t.Fatal("bad dsn")
	}
	t.Log("inserting into ProxySQL")
	conn.Conn().Exec("insert into mysql_servers (hostgroup_id, hostname, max_connections) values (0, 'readerHost', 1000)")
	if exists, _ := conn.HostExists("readerHost"); !exists {
		t.Log("readerHost was inserted but not read")
		t.Fail()
	}
}

func TestHostExistsReturnsFalseForNonExistentHost(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := New(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")), 0, 1)
	if err != nil {
		t.Fatal("bad dsn")
	}
	if exists, _ := conn.HostExists("readerHost"); exists {
		t.Log("readerHost was not inserted but read")
		t.Fail()
	}
}

func TestAllReturnsAllEntries(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := New(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")), 0, 1)
	if err != nil {
		t.Fatal("bad dsn")
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
		t.Fatal("err while getting all entries")
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
	defer SetupAndTeardownProxySQL(t)()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := New(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")), 0, 1)
	if err != nil {
		t.Fatal("bad dsn")
	}
	entries, err := conn.All()
	if err != nil {
		t.Fatal("err while getting all entries")
	}
	if len(entries) != 0 {
		t.Logf("entries is nonzero for empty table: %v", entries)
		t.Fail()
	}
}

func TestAddHostAddsAHost(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := New(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")), 0, 1)
	if err != nil {
		t.Fatal("bad dsn")
	}
	conn.AddHost("some-host", 3, 1000)
	var hostname string
	var hostgroup int
	conn.conn.QueryRow("select hostname, hostgroup_id from mysql_servers").Scan(&hostname, &hostgroup)
	if hostname != "some-host" || hostgroup != 3 {
		t.Logf("hostname or hostgroup read were not the ones in AddHost %s, %d", hostname, hostgroup)
		t.Fail()
	}
}

func TestRemoveHostRemovesAHost(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := New(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")), 0, 1)
	if err != nil {
		t.Fatal("bad dsn")
	}

	_, err = conn.Conn().Exec("insert into mysql_servers (hostgroup_id, hostname, max_connections) values (0, 'some-host', 1000)")
	if err != nil {
		t.Fatalf("err setting up test: %v", err)
	}

	if err := conn.RemoveHost("some-host"); err != nil {
		t.Fatal("err removing host %v", err)
	}

	exists, err := conn.HostExists("some-host")
	if err != nil {
		t.Fatalf("err checking existence of host: %v", err)
	}

	if exists {
		t.Log("host still existed after removal")
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
	if testing.Short() {
		t.Skip()
	}
	t.Log("setting up proxysql container")
	var err error
	proxysqlContainer, err = pool.Run("kirinrastogi/proxysql", "latest", []string{})
	if err != nil {
		t.Fatalf("could not build and run proxysql, is dockerd running? error: %v", err)
	}
	t.Log("ran proxysql container, waiting for network connection")
	time.Sleep(600 * time.Millisecond)
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

func resetExec() {
	exec = func(p *ProxySQL, query string, args ...interface{}) (sql.Result, error) {
		return p.conn.Exec(query)
	}
}
