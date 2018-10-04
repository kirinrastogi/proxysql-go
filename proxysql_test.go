package proxysql

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/ory/dockertest"
	"log"
	"math/rand"
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
	rand.Seed(time.Now().UTC().UnixNano())
	code := m.Run()
	os.Exit(code)
}

func TestNewWithHostgroupsSetsHostgroups(t *testing.T) {
	conn, err := New("some-dsn", 33, 86, "mysql_servers")
	if err != nil {
		t.Fatal(err)
	}
	if conn.writerHostgroup != 33 || conn.readerHostgroup != 86 {
		t.Logf("hostgroups were not set properly: w: %d, r: %d", conn.writerHostgroup, conn.readerHostgroup)
		t.Fail()
	}
}

func TestNewWithDefaultsSetsDefaultHostgroups(t *testing.T) {
	conn, err := NewWithDefaults("some-dsn")
	if err != nil {
		t.Fatal(err)
	}
	if conn.writerHostgroup != 0 || conn.readerHostgroup != 1 {
		t.Log("default hostgroups were not 0 and 1")
		t.Fail()
	}
}

func TestNewWithDefaultsSetsDefaultTable(t *testing.T) {
	conn, err := NewWithDefaults("some-dsn")
	if err != nil {
		t.Fatal(err)
	}
	if conn.table != "mysql_servers" {
		t.Fatalf("default table was not set to mysql_servers, was %s instead", conn.table)
	}
}

func TestNewErrorsOnSqlOpenError(t *testing.T) {
	open = func(driver string, dsn string) (*sql.DB, error) {
		return nil, errors.New("Error creating connection pool")
	}
	defer resetOpen()
	_, err := New("some-dsn", 0, 1, "mysql_servers")
	if err == nil {
		t.Log("New did not propogate err")
		t.Fail()
	}
}

func TestNewWithDefaultsErrorsOnSqlOpenError(t *testing.T) {
	open = func(driver string, dsn string) (*sql.DB, error) {
		return nil, errors.New("Error creating connection pool")
	}
	defer resetOpen()
	_, err := NewWithDefaults("some-dsn")
	if err == nil {
		t.Log("New did not propogate err")
		t.Fail()
	}
}

func TestSetTableSetsTheTable(t *testing.T) {
	conn, err := NewWithDefaults("some-dsn")
	if err != nil {
		t.Fatal(err)
	}
	expected := "runtime_mysql_servers"
	conn.SetTable(expected)
	if conn.table != expected {
		t.Fatalf("table was not set correctly to %s", expected)
	}
}

func TestTableGetsTheTable(t *testing.T) {
	expected := "runtime_mysql_servers"
	conn, err := New("some-dsn", 33, 86, expected)
	if err != nil {
		t.Fatal(err)
	}
	if conn.Table() != expected {
		t.Fatalf("Table did not return table that was set: %s", expected)
	}
}

func TestSetTableErrorsOnBadTableName(t *testing.T) {
	conn, err := NewWithDefaults("some-dsn")
	if err != nil {
		t.Fatal(err)
	}
	if conn.SetTable("some table") == nil {
		t.Fatal("set table did not error with bad table name passed")
	}
}

func TestPingSucceedsOnLiveContainer(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := NewWithDefaults(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
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
	conn, err := NewWithDefaults(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
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
	conn, err := NewWithDefaults(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
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
	conn, err := NewWithDefaults(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
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
	conn, err := NewWithDefaults(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
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
	conn, err := NewWithDefaults(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
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
	conn, err := NewWithDefaults(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
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
	conn, err := NewWithDefaults(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
	if err != nil {
		t.Fatal("bad dsn")
	}

	queryType := ""
	fullQuery := ""
	// when SetWriter execs, make it return the first word in query
	exec = func(p *ProxySQL, queryString string, args ...interface{}) (sql.Result, error) {
		queryType = strings.Split(queryString, " ")[0]
		fullQuery = queryString
		return p.conn.Exec(queryString)
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
	conn, err := NewWithDefaults(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
	if err != nil {
		t.Fatal("bad dsn")
	}
	exec = func(_ *ProxySQL, _ string, _ ...interface{}) (sql.Result, error) {
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
	conn, err := NewWithDefaults(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
	if err != nil {
		t.Fatal("bad dsn")
	}

	// when SetWriter execs to update, make it return the first word in query
	exec = func(_ *ProxySQL, queryString string, _ ...interface{}) (sql.Result, error) {
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
	conn, err := NewWithDefaults(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
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
	conn, err := NewWithDefaults(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
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
	conn, err := NewWithDefaults(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
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
		insertQuery := fmt.Sprintf("insert into mysql_servers (hostgroup_id, hostname, max_connections) values (%d, '%s', 1000)", hostgroup, hostname)
		conn.Conn().Exec(insertQuery)
	}
	entries, err := conn.All()
	if err != nil {
		t.Fatalf("err while getting all entries: %v", err)
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
	conn, err := NewWithDefaults(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
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

func TestAllErrorsOnQueryError(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	defer resetQuery()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := NewWithDefaults(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
	if err != nil {
		t.Fatal("bad dsn")
	}
	query = func(*ProxySQL, string, ...interface{}) (*sql.Rows, error) {
		return nil, errors.New("error querying proxysql")
	}
	_, err = conn.Conn().Exec("insert into mysql_servers (hostgroup_id, hostname, max_connections) values (0, 'writerHost', 1000)")
	if err != nil {
		t.Fatalf("error setting up test: %v", err)
	}
	entries, err := conn.All()
	if entries != nil || err == nil {
		t.Logf("entries or err was not nil: %v, %v", entries, err)
		t.Fail()
	}
}

func TestAllErrorsOnScanError(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	defer resetScanRows()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := NewWithDefaults(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
	if err != nil {
		t.Fatal("bad dsn")
	}
	scanRows = func(_ *sql.Rows, dest ...interface{}) error {
		return fmt.Errorf("error scanning values: %v", dest...)
	}
	_, err = conn.Conn().Exec("insert into mysql_servers (hostgroup_id, hostname, max_connections) values (0, 'writerHost', 1000)")
	if err != nil {
		t.Fatalf("error setting up test: %v", err)
	}
	entries, err := conn.All()
	if entries != nil || err == nil {
		t.Logf("entries or err was not nil: %v, %v", entries, err)
		t.Fail()
	}
}

func TestAllErrorsOnRowsError(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	defer resetRowsErr()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := NewWithDefaults(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
	if err != nil {
		t.Fatal("bad dsn")
	}
	rowsErr = func(_ *sql.Rows) error {
		return errors.New("error reading rows")
	}
	_, err = conn.Conn().Exec("insert into mysql_servers (hostgroup_id, hostname, max_connections) values (0, 'writerHost', 1000)")
	if err != nil {
		t.Fatalf("error setting up test: %v", err)
	}
	entries, err := conn.All()
	if entries != nil || err == nil {
		t.Logf("entries or err was not nil: %v, %v", entries, err)
		t.Fail()
	}
}

func TestAddHostAddsAHost(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := NewWithDefaults(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
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
	conn, err := NewWithDefaults(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
	if err != nil {
		t.Fatal("bad dsn")
	}

	_, err = conn.Conn().Exec("insert into mysql_servers (hostgroup_id, hostname, max_connections) values (0, 'some-host', 1000)")
	if err != nil {
		t.Fatalf("err setting up test: %v", err)
	}

	if err := conn.RemoveHost("some-host"); err != nil {
		t.Fatalf("err removing host %v", err)
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

func TestRemoveHostFromHostgroupRemovesAHostFromSpecificHostgroup(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := NewWithDefaults(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
	if err != nil {
		t.Fatal("bad dsn")
	}

	for i := 0; i < 3; i++ {
		insertQuery := fmt.Sprintf("insert into mysql_servers (hostgroup_id, hostname, max_connections) values (%d, 'some-host', 1000)", i)
		_, err = conn.Conn().Exec(insertQuery)
		if err != nil {
			t.Fatalf("err setting up test: %v", err)
		}
	}

	if err := conn.RemoveHostFromHostgroup("some-host", 1); err != nil {
		t.Fatalf("err removing host %v", err)
	}

	exists, err := conn.HostExists("some-host")
	if err != nil {
		t.Fatalf("err checking existence of host: %v", err)
	}
	if !exists {
		t.Log("all entries for host were deleted")
		t.Fail()
	}
}

func TestPersistChangesErrorsOnSave(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	defer resetExec()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := NewWithDefaults(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
	if err != nil {
		t.Fatal("bad dsn")
	}
	saveErr := errors.New("could not save servers to disk")
	exec = func(_ *ProxySQL, queryString string, _ ...interface{}) (sql.Result, error) {
		if queryString == "save mysql servers to disk" {
			return nil, saveErr
		}
		return nil, nil
	}
	err = conn.PersistChanges()
	if err != saveErr {
		t.Log("persist changes did not error on save failure")
		t.Fail()
	}
}

func TestPersistChangesErrorsOnLoad(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	defer resetExec()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := NewWithDefaults(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
	if err != nil {
		t.Fatal("bad dsn")
	}
	loadErr := errors.New("error saving servers to disk")
	exec = func(_ *ProxySQL, queryString string, _ ...interface{}) (sql.Result, error) {
		if queryString == "load mysql servers to runtime" {
			return nil, loadErr
		}
		return nil, nil
	}
	err = conn.PersistChanges()
	if err != loadErr {
		t.Log("persist changes did not error on load failure")
		t.Fail()
	}
}

func TestSizeOfHostgroupReturnsSize(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := NewWithDefaults(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
	if err != nil {
		t.Fatal("bad dsn")
	}
	t.Log("inserting into ProxySQL")
	entryCount := rand.Intn(10)
	for i := 0; i < entryCount; i++ {
		insertQuery := fmt.Sprintf("insert into mysql_servers (hostgroup_id, hostname, max_connections) values (4, 'host-num%d', 1000)", i)
		_, err = conn.Conn().Exec(insertQuery)
		if err != nil {
			t.Fatalf("err setting up test: %v", err)
		}
	}
	size, err := conn.SizeOfHostgroup(4)
	if err != nil {
		t.Logf("unexpected err reading hostgroup size: %v", err)
		t.Fail()
	}

	if size != entryCount {
		t.Logf("hostgroup read does not match amount of entries %d != %d", size, entryCount)
		t.Fail()
	}
}

func TestSizeOfHostgroupErrorsOnQueryError(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	defer resetScanRow()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := NewWithDefaults(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
	if err != nil {
		t.Fatal("bad dsn")
	}
	queryErr := errors.New("error counting entries")
	scanRow = func(*sql.Row, ...interface{}) error {
		return queryErr
	}
	size, err := conn.SizeOfHostgroup(1)
	if err != queryErr {
		t.Logf("size of hostgroup did not properly error on failed count: %v", err)
		t.Fail()
	}

	if size != -1 {
		t.Logf("size of hostgroup was not returned as -1 in error: %d", size)
		t.Fail()
	}
}

func TestTableUsedIsTheOneSet(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	defer resetExec()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := New(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")), 0, 1, "runtime_mysql_servers")
	if err != nil {
		t.Fatal("bad dsn")
	}
	var queryBuilt string
	exec = func(p *ProxySQL, query string, _ ...interface{}) (sql.Result, error) {
		queryBuilt = query
		return nil, nil
	}
	err = conn.RemoveHost("some-host")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if queryBuilt != "delete from runtime_mysql_servers where hostname = 'some-host'" {
		t.Logf("query built is not the expected one: %s", queryBuilt)
		t.Fail()
	}
}

func TestPersistChangesLoadsConfigurationToRuntime(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	base := "remote-admin:password@tcp(localhost:%s)/"
	containerAddr := fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp"))
	conn, err := NewWithDefaults(containerAddr)
	if err != nil {
		t.Log("bad dsn")
		t.Fail()
	}
	// make entries map compare to runtime_servers.All()
	entries := map[string]int{
		"reader1": 1,
		"reader2": 1,
		"writer":  0,
	}
	t.Log("inserting into ProxySQL")
	for hostname, hostgroup := range entries {
		conn.AddHost(hostname, hostgroup, 1000)
	}
	err = conn.PersistChanges()
	if err != nil {
		t.Fatalf("could not persist changes: %v", err)
	}
	runtime_conn, err := New(containerAddr, 0, 1, "runtime_mysql_servers")
	runtime_servers, err := runtime_conn.All()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !hostgroupsEqual(entries, runtime_servers) {
		t.Log("changes were not persisted from mysql_servers to runtime_mysql_servers")
		t.Logf("table %v != %v", entries, runtime_servers)
		t.Fail()
	}
}

func hostgroupsEqual(map1 map[string]int, map2 map[string]int) bool {
	for k, v := range map1 {
		val, ok := map2[k]
		if !ok {
			return false
		}
		if val != v {
			return false
		}
		delete(map2, k)
		delete(map1, k)
	}
	return len(map2) == 0 && len(map1) == 0
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
	exec = func(p *ProxySQL, queryString string, _ ...interface{}) (sql.Result, error) {
		return p.conn.Exec(queryString)
	}
}

func resetQuery() {
	query = func(p *ProxySQL, queryString string, _ ...interface{}) (*sql.Rows, error) {
		return p.conn.Query(queryString)
	}
}

func resetScanRows() {
	scanRows = func(rs *sql.Rows, dest ...interface{}) error {
		return rs.Scan(dest...)
	}
}

func resetScanRow() {
	scanRow = func(rs *sql.Row, dest ...interface{}) error {
		return rs.Scan(dest...)
	}
}

func resetRowsErr() {
	rowsErr = func(rs *sql.Rows) error {
		return rs.Err()
	}
}
