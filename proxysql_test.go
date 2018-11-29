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
	"reflect"
	"sort"
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

func TestNewSetsDSN(t *testing.T) {
	p, err := NewProxySQL("dsn")
	if err != nil {
		t.Logf("unexpected err: %v", err)
		t.Fail()
	}

	if p.dsn != "dsn" {
		t.Fatalf("dsn received was not expected: %s", p.dsn)
	}
}

func TestNewErrorsOnSqlOpenError(t *testing.T) {
	open = func(driver string, dsn string) (*sql.DB, error) {
		return nil, errors.New("Error creating connection pool")
	}
	defer resetOpen()
	_, err := NewProxySQL("some-dsn")
	if err == nil {
		t.Log("New did not propogate err")
		t.Fail()
	}
}

func TestPingSucceedsOnLiveContainer(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := NewProxySQL(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
	if err != nil {
		t.Fatal("bad dsn")
	}
	if err := conn.Ping(); err != nil {
		t.Logf("ping failed to live container at %s", base)
		t.Fail()
	}
}

func TestPingFailsOnDeadContainer(t *testing.T) {
	conn, err := NewProxySQL("dsn")
	if err != nil {
		t.Fatal("bad dsn")
	}
	if err := conn.Ping(); err == nil {
		t.Fatal("ping succeeded to bad dsn")
	}
}

func TestCloseClosesConnectionToProxySQL(t *testing.T) {
	SetupProxySQL(t)
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := NewProxySQL(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
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

func TestAllReturnsAllEntries(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := NewProxySQL(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
	if err != nil {
		t.Fatal("bad dsn")
	}
	t.Log("inserting into ProxySQL")
	insertedEntries := []*Host{
		defaultHost().Hostname("hostname1"),
		defaultHost().Hostname("hostname2").Port(3307),
		defaultHost().Hostname("hostname3").Port(3305),
	}
	err = conn.AddHosts(insertedEntries...)
	entries, err := conn.All()
	if err != nil {
		t.Fatalf("err while getting all entries: %v", err)
	}
	// assert the two arrays entries and insertedEntries are deep equal
	for i := 0; i < len(insertedEntries); i++ {
		if !reflect.DeepEqual(insertedEntries[i], entries[i]) {
			t.Fatalf("returned hosts not equal to inserted hosts: %v != %v", insertedEntries[i], entries[i])
		}
	}
}

func TestAllReturnsEmptyMapForEmptyTable(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := NewProxySQL(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
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

func TestAllErrorsOnParseErrorOfTable(t *testing.T) {
	conn, err := NewProxySQL("dsn")
	if err != nil {
		t.Fatal("bad dsn")
	}
	entries, err := conn.All(Table("not a real table"))
	if err == nil {
		t.Fatalf("did not get error when specifying bad table")
	}

	if entries != nil {
		t.Fatalf("received non nil list of hosts on error: %v", entries)
	}
}

func TestAllErrorsOnQueryError(t *testing.T) {
	defer resetQuery()
	conn, err := NewProxySQL("/")
	if err != nil {
		t.Fatal("bad dsn")
	}
	query = func(*ProxySQL, string, ...interface{}) (*sql.Rows, error) {
		return nil, errors.New("error querying proxysql")
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
	conn, err := NewProxySQL(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
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
	conn, err := NewProxySQL(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
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
	conn, err := NewProxySQL(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
	if err != nil {
		t.Fatal("bad dsn")
	}
	err = conn.AddHost(Hostname("some-host"))
	if err != nil {
		t.Logf("unexpected err adding host: %v", err)
		t.Fail()
	}
	var hostname string
	var hostgroup int
	conn.conn.QueryRow("select hostname, hostgroup_id from mysql_servers").Scan(&hostname, &hostgroup)
	if hostname != "some-host" || hostgroup != 0 {
		t.Logf("hostname or hostgroup read were not the ones in AddHost %s, %d", hostname, hostgroup)
		t.Fail()
	}
}

func TestAddHostAddsAHostToHostgroup(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := NewProxySQL(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
	if err != nil {
		t.Fatal("bad dsn")
	}
	err = conn.AddHost(Hostname("some-host"), Hostgroup(1))
	if err != nil {
		t.Logf("unexpected err adding host: %v", err)
		t.Fail()
	}
	var hostname string
	var hostgroup int
	conn.conn.QueryRow("select hostname, hostgroup_id from mysql_servers").Scan(&hostname, &hostgroup)
	if hostname != "some-host" || hostgroup != 1 {
		t.Logf("hostname or hostgroup read were not the ones in AddHost %s, %d", hostname, hostgroup)
		t.Fail()
	}
}

func TestAddHostReturnsErrorOnBadConfig(t *testing.T) {
	conn, err := NewProxySQL("/")
	if err != nil {
		t.Fatal("bad dsn")
	}
	err = conn.AddHost(Hostname("some-host"), Hostgroup(1), Port(-1))
	if err != ErrConfigBadPort {
		t.Logf("did not receive err about bad port: %v", err)
		t.Fail()
	}
}

func TestAddHostsReturnsErrorOnError(t *testing.T) {
	defer resetExec()
	conn, err := NewProxySQL("/")
	if err != nil {
		t.Fatal("bad dsn")
	}
	mockErr := errors.New("mock")
	exec = func(_ *ProxySQL, queryString string, _ ...interface{}) (sql.Result, error) {
		return nil, mockErr
	}
	err = conn.AddHosts(defaultHost())
	if err != mockErr {
		t.Fatalf("did not get expected error: %v", err)
	}
}

func TestClearClearsProxySQL(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := NewProxySQL(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
	if err != nil {
		t.Fatal("bad dsn")
	}
	conn.AddHost(Hostname("h1"))
	conn.Clear()
	entries, _ := conn.All()
	if len(entries) != 0 {
		t.Fatalf("entries was not empty after clearing: %v", entries)
	}
}

func TestRemoveHostRemovesAHost(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := NewProxySQL(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
	if err != nil {
		t.Fatal("bad dsn")
	}

	host := defaultHost().Hostname("some-host").Hostgroup(0)
	err = conn.AddHosts(host)
	if err != nil {
		t.Fatalf("err setting up test: %v", err)
	}

	if err := conn.RemoveHost(host); err != nil {
		t.Fatalf("err removing host %v", err)
	}

	hosts, err := conn.HostsLike(Hostname("some-host"))
	if err != nil {
		t.Fatalf("err checking existence of host: %v", err)
	}

	if len(hosts) != 0 {
		t.Logf("%v", hosts)
		t.Log("host still existed after removal")
		t.Fail()
	}
}

func TestRemoveHostsLikeRemovesHostsLike(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := NewProxySQL(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
	if err != nil {
		t.Fatal("bad dsn")
	}
	conn.AddHost(Hostname("a"))
	conn.AddHost(Hostname("b"))
	conn.AddHost(Hostname("b"), Hostgroup(1))
	conn.AddHost(Hostname("c"), Hostgroup(1))
	conn.RemoveHostsLike(Hostgroup(1))
	entries, err := conn.All()
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].hostname < entries[j].hostname
	})
	if len(entries) != 2 || entries[0].hostname != "a" || entries[1].hostname != "b" {
		t.Fatalf("did not remove two entries: %v", entries)
	}
}

func TestRemoveHostsLikeErrorsOnParseOrExecError(t *testing.T) {
	defer resetExec()
	conn, err := NewProxySQL("dsn")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	err = conn.RemoveHostsLike(Hostgroup(-1))
	if err != ErrConfigBadHostgroup {
		t.Fatalf("did not receive validation error on bad param: %v", err)
	}

	mockErr := errors.New("mock")
	exec = func(_ *ProxySQL, _ string, _ ...interface{}) (sql.Result, error) {
		return nil, mockErr
	}
	err = conn.RemoveHostsLike(Hostgroup(1))
	if err != mockErr {
		t.Fatalf("did not propogate execution error: %v", err)
	}
}

func TestRemoveHostsRemovesAllHostsSpecified(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := NewProxySQL(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
	if err != nil {
		t.Fatal("bad dsn")
	}
	conn.AddHost(Hostname("b"), Hostgroup(1))
	conn.AddHost(Hostname("c"), Hostgroup(1))
	entries, _ := conn.All()
	conn.RemoveHosts(entries...)
	entries, _ = conn.All()
	if len(entries) != 0 {
		t.Fatalf("entries returned is not empty: %v", entries)
	}
}

func TestRemoveHostsPropogatesErrorFromRemoveHost(t *testing.T) {
	defer resetExec()
	conn, _ := NewProxySQL("dsn")
	mockErr := errors.New("mock")
	exec = func(_ *ProxySQL, _ string, _ ...interface{}) (sql.Result, error) {
		return nil, mockErr
	}

	if err := conn.RemoveHosts(defaultHost()); err != mockErr {
		t.Fatalf("unexpected error from RemoveHosts, did not propogate: %v", err)
	}
}

func TestHostsLike(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := NewProxySQL(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
	if err != nil {
		t.Fatal("bad dsn")
	}

	conn.AddHost(Hostname("hostname3"), Hostgroup(3))
	conn.AddHost(Hostname("hostname1"), Hostgroup(1))
	conn.AddHost(Hostname("hostname2"), Hostgroup(1))

	hosts, err := conn.HostsLike(Hostgroup(1))
	if err != nil {
		t.Fatalf("err checking existence of host: %v", err)
	}

	if len(hosts) != 2 {
		t.Fatalf("did not receive expected amount of hosts: %v", hosts)
	}
}

func TestHostsLikeReturnsErrorOnRowScanError(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	defer resetScanRows()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := NewProxySQL(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
	if err != nil {
		t.Fatal("bad dsn")
	}

	conn.AddHost(Hostname("hostname1"), Hostgroup(1))
	conn.AddHost(Hostname("hostname2"), Hostgroup(1))

	mockErr := errors.New("mock")
	scanRows = func(_ *sql.Rows, _ ...interface{}) error {
		return mockErr
	}

	hosts, err := conn.HostsLike(Hostgroup(1))

	if err != mockErr {
		t.Fatalf("did not receive error when scanRows returned error: %v", err)
	}

	if hosts != nil {
		t.Fatalf("did not receive nil slice on error: %v", hosts)
	}
}

func TestHostsLikeReturnsErrorOnRowsError(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	defer resetRowsErr()
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := NewProxySQL(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
	if err != nil {
		t.Fatal("bad dsn")
	}

	conn.AddHost(Hostname("hostname1"), Hostgroup(1))
	conn.AddHost(Hostname("hostname2"), Hostgroup(1))

	mockErr := errors.New("mock")
	rowsErr = func(_ *sql.Rows) error {
		return mockErr
	}

	hosts, err := conn.HostsLike(Hostgroup(1))

	if err != mockErr {
		t.Fatalf("did not receive error when scanRows returned error: %v", err)
	}

	if hosts != nil {
		t.Fatalf("did not receive nil slice on error: %v", hosts)
	}
}

func TestHostsLikeParseErrorAndQueryErrorReturnErrors(t *testing.T) {
	defer resetQuery()
	conn, err := NewProxySQL("dsn")
	_, err = conn.HostsLike(Port(-1))
	if err != ErrConfigBadPort {
		t.Fatalf("did not receive expected error on supplying bad parameters to HostsLike: %v", err)
	}

	mockErr := errors.New("mock")
	query = func(_ *ProxySQL, _ string, _ ...interface{}) (*sql.Rows, error) {
		return nil, mockErr
	}
	_, err = conn.HostsLike(Hostname("yee"))
	if err != mockErr {
		t.Fatalf("did not receive expected error when query returned error: %v", err)
	}
}

func TestPersistChangesErrorsOnSave(t *testing.T) {
	defer resetExec()
	conn, err := NewProxySQL("/")
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
	conn, err := NewProxySQL(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
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

func TestPersistChangesLoadsConfigurationToRuntime(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	base := "remote-admin:password@tcp(localhost:%s)/"
	containerAddr := fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp"))
	conn, err := NewProxySQL(containerAddr)
	if err != nil {
		t.Log("bad dsn")
		t.Fail()
	}
	// make entries map compare to runtime_servers.All()
	entries := []*Host{
		defaultHost().Hostname("reader1").Hostgroup(1),
		defaultHost().Hostname("reader2").Hostgroup(1),
		defaultHost().Hostname("writer").Hostgroup(0),
	}
	t.Log("inserting into ProxySQL")
	conn.AddHosts(entries...)
	err = conn.PersistChanges()
	if err != nil {
		t.Fatalf("could not persist changes: %v", err)
	}
	runtime_servers, err := conn.All(Table("runtime_mysql_servers"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].hostname < entries[j].hostname
	})
	sort.Slice(runtime_servers, func(i, j int) bool {
		return runtime_servers[i].hostname < runtime_servers[j].hostname
	})
	for i := 0; i < len(entries); i++ {
		if !reflect.DeepEqual(entries[i], runtime_servers[i]) {
			t.Log("changes were not persisted from mysql_servers to runtime_mysql_servers")
			t.Logf("table %v != %v", entries[i], runtime_servers[i])
			t.Fail()
		}
	}
}

func TestAddHostWithAllConfigurationsAddsAHostConfigured(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	base := "remote-admin:password@tcp(localhost:%s)/"
	containerAddr := fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp"))
	conn, err := NewProxySQL(containerAddr)
	if err != nil {
		t.Log("bad dsn")
		t.Fail()
	}
	var (
		port            = 3307
		hostname        = "mysql-1"
		max_connections = 300
		hostgroup_id    = 1
		status          = "OFFLINE_SOFT"
		weight          = 2
	)
	err = conn.AddHost(Table("mysql_servers"), Port(port), Hostname(hostname), MaxConnections(max_connections), Hostgroup(hostgroup_id), Status(status), Weight(weight))
	if err != nil {
		t.Fatalf("error setting up test: %v", err)
	}

	entries, err := conn.All()
	if len(entries) != 1 || err != nil {
		t.Fatalf("error getting all: %v, entries: %v", err, entries)
	}
	host := entries[0]

	if host.port != port {
		t.Fatalf("port not set properly: %d", host.port)
	}

	if host.hostname != hostname {
		t.Fatalf("hostname not set properly: %s", host.hostname)
	}
	if host.max_connections != max_connections {
		t.Fatalf("max_connections not set properly: %d", host.max_connections)
	}

	if host.hostgroup_id != hostgroup_id {
		t.Fatalf("hostgroup_id not set properly: %d", host.hostgroup_id)
	}

	if host.status != status {
		t.Fatalf("status not set properly: %s", host.status)
	}

	if host.weight != weight {
		t.Fatalf("weight not set properly: %d", host.weight)
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

func resetRowsErr() {
	rowsErr = func(rs *sql.Rows) error {
		return rs.Err()
	}
}
