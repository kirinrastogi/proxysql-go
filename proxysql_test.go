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

func shortSetup(t *testing.T) *ProxySQL {
	conn, err := NewProxySQL("/")
	if err != nil {
		t.Fatal("bad dsn")
	}
	return conn
}

func longSetup(t *testing.T) *ProxySQL {
	base := "remote-admin:password@tcp(localhost:%s)/"
	conn, err := NewProxySQL(fmt.Sprintf(base, proxysqlContainer.GetPort("6032/tcp")))
	if err != nil {
		t.Fatal("bad dsn")
	}
	return conn
}

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
	p, err := NewProxySQL("/")
	if err != nil {
		t.Logf("unexpected err: %v", err)
		t.Fail()
	}

	if p.dsn != "/" {
		t.Fatalf("dsn received was not expected: %s", p.dsn)
	}
}

func TestNewErrorsOnSqlOpenError(t *testing.T) {
	open = func(driver string, dsn string) (*sql.DB, error) {
		return nil, errors.New("Error creating connection pool")
	}
	defer resetHelpers()
	_, err := NewProxySQL("some-dsn")
	if err == nil {
		t.Log("New did not propagate err")
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
	conn := shortSetup(t)
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
	conn := longSetup(t)
	t.Log("inserting into ProxySQL")
	insertedEntries := []*Host{
		DefaultHost().SetHostname("hostname1"),
		DefaultHost().SetHostname("hostname2").SetPort(3307),
		DefaultHost().SetHostname("hostname3").SetPort(3305),
	}
	conn.AddHosts(insertedEntries...)
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
	conn := longSetup(t)
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
	conn := shortSetup(t)
	entries, err := conn.All(Table("not a real table"))
	if err == nil {
		t.Fatalf("did not get error when specifying bad table")
	}
	if entries != nil {
		t.Fatalf("received non nil list of hosts on error: %v", entries)
	}
}

// Only proxysql.All(Table("name")) is allowed, no other opts
func TestAllErrorsWhenQueryOptsAdded(t *testing.T) {
	conn, err := NewProxySQL("/")
	if err != nil {
		t.Fatal("bad dsn")
	}
	_, err = conn.All(Table("runtime_mysql_servers"), HostgroupID(1))
	if err == nil {
		t.Fatalf("did not get error when specifying hostgroup_id")
	}
	t.Log(err)
}

func TestAllErrorsOnQueryError(t *testing.T) {
	defer resetHelpers()
	conn := shortSetup(t)
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
	defer resetHelpers()
	conn := longSetup(t)
	scanRows = func(_ *sql.Rows, dest ...interface{}) error {
		return fmt.Errorf("error scanning values: %v", dest...)
	}
	_, err := conn.Conn().Exec("insert into mysql_servers (hostgroup_id, hostname, max_connections) values (0, 'writerHost', 1000)")
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
	defer resetHelpers()
	conn := longSetup(t)
	rowsErr = func(_ *sql.Rows) error {
		return errors.New("error reading rows")
	}
	_, err := conn.Conn().Exec("insert into mysql_servers (hostgroup_id, hostname, max_connections) values (0, 'writerHost', 1000)")
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
	conn := longSetup(t)
	err := conn.AddHost(Hostname("some-host"))
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

func TestAddHostAddsAHostToHostgroupID(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	conn := longSetup(t)
	err := conn.AddHost(Hostname("some-host"), HostgroupID(1))
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
	conn := shortSetup(t)
	err := conn.AddHost(Hostname("some-host"), HostgroupID(1), Port(-1))
	if err != ErrConfigBadPort {
		t.Logf("did not receive err about bad port: %v", err)
		t.Fail()
	}
}

func TestAddHostsReturnsErrorOnError(t *testing.T) {
	defer resetHelpers()
	conn := shortSetup(t)
	mockErr := errors.New("mock")
	exec = func(_ *ProxySQL, queryString string, _ ...interface{}) (sql.Result, error) {
		return nil, mockErr
	}
	err := conn.AddHosts(DefaultHost())
	if err != mockErr {
		t.Fatalf("did not get expected error: %v", err)
	}
}

func TestAddHostsReturnsErrorBeforeConnectingToProxySQLOnInvalidHost(t *testing.T) {
	conn := shortSetup(t)
	host := DefaultHost().SetHostgroupID(-1)
	err := conn.AddHosts(host)
	if err != ErrConfigBadHostgroupID {
		t.Fatalf("did not get expected error of bad hostgroupid when validating")
	}
	t.Log(err)
}

func TestClearClearsProxySQL(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	conn := longSetup(t)
	conn.AddHost(Hostname("h1"))
	conn.Clear()
	entries, _ := conn.All()
	if len(entries) != 0 {
		t.Fatalf("entries was not empty after clearing: %v", entries)
	}
}

func TestRemoveHostRemovesAHost(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	conn := longSetup(t)
	host := DefaultHost().SetHostname("some-host").SetHostgroupID(0)
	err := conn.AddHosts(host)
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
	conn := longSetup(t)
	conn.AddHost(Hostname("a"))
	conn.AddHost(Hostname("b"))
	conn.AddHost(Hostname("b"), HostgroupID(1))
	conn.AddHost(Hostname("c"), HostgroupID(1))
	conn.RemoveHostsLike(HostgroupID(1))
	entries, _ := conn.All()
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].hostname < entries[j].hostname
	})
	if len(entries) != 2 || entries[0].hostname != "a" || entries[1].hostname != "b" {
		t.Fatalf("did not remove two entries: %v", entries)
	}
}

func TestRemoveHostsLikeErrorsOnParseOrExecError(t *testing.T) {
	defer resetHelpers()
	conn := shortSetup(t)
	err := conn.RemoveHostsLike(HostgroupID(-1))
	if err != ErrConfigBadHostgroupID {
		t.Fatalf("did not receive validation error on bad param: %v", err)
	}

	mockErr := errors.New("mock")
	exec = func(_ *ProxySQL, _ string, _ ...interface{}) (sql.Result, error) {
		return nil, mockErr
	}
	err = conn.RemoveHostsLike(HostgroupID(1))
	if err != mockErr {
		t.Fatalf("did not propagate execution error: %v", err)
	}
}

func TestRemoveHostsRemovesAllHostsSpecified(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	conn := longSetup(t)
	conn.AddHost(Hostname("b"), HostgroupID(1))
	conn.AddHost(Hostname("c"), HostgroupID(1))
	entries, _ := conn.All()
	conn.RemoveHosts(entries...)
	entries, _ = conn.All()
	if len(entries) != 0 {
		t.Fatalf("entries returned is not empty: %v", entries)
	}
}

func TestRemoveHostsPropagatesErrorFromRemoveHost(t *testing.T) {
	defer resetHelpers()
	conn := shortSetup(t)
	mockErr := errors.New("mock")
	exec = func(_ *ProxySQL, _ string, _ ...interface{}) (sql.Result, error) {
		return nil, mockErr
	}

	if err := conn.RemoveHosts(DefaultHost()); err != mockErr {
		t.Fatalf("unexpected error from RemoveHosts, did not propagate: %v", err)
	}
}

func TestHostsLike(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	conn := longSetup(t)
	conn.AddHost(Hostname("hostname3"), HostgroupID(3))
	conn.AddHost(Hostname("hostname1"), HostgroupID(1))
	conn.AddHost(Hostname("hostname2"), HostgroupID(1))
	hosts, err := conn.HostsLike(HostgroupID(1))
	if err != nil {
		t.Fatalf("err checking existence of host: %v", err)
	}
	if len(hosts) != 2 {
		t.Fatalf("did not receive expected amount of hosts: %v", hosts)
	}
}

func TestHostsLikeReturnsErrorOnRowScanError(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	defer resetHelpers()
	conn := longSetup(t)
	conn.AddHost(Hostname("hostname1"), HostgroupID(1))
	conn.AddHost(Hostname("hostname2"), HostgroupID(1))
	mockErr := errors.New("mock")
	scanRows = func(_ *sql.Rows, _ ...interface{}) error {
		return mockErr
	}
	hosts, err := conn.HostsLike(HostgroupID(1))
	if err != mockErr {
		t.Fatalf("did not receive error when scanRows returned error: %v", err)
	}
	if hosts != nil {
		t.Fatalf("did not receive nil slice on error: %v", hosts)
	}
}

func TestHostsLikeReturnsErrorOnRowsError(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	defer resetHelpers()
	conn := longSetup(t)
	conn.AddHost(Hostname("hostname1"), HostgroupID(1))
	conn.AddHost(Hostname("hostname2"), HostgroupID(1))
	mockErr := errors.New("mock")
	rowsErr = func(_ *sql.Rows) error {
		return mockErr
	}
	hosts, err := conn.HostsLike(HostgroupID(1))
	if err != mockErr {
		t.Fatalf("did not receive error when scanRows returned error: %v", err)
	}
	if hosts != nil {
		t.Fatalf("did not receive nil slice on error: %v", hosts)
	}
}

func TestHostsLikeParseErrorAndQueryErrorReturnErrors(t *testing.T) {
	defer resetHelpers()
	conn := shortSetup(t)
	_, err := conn.HostsLike(Port(-1))
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
	defer resetHelpers()
	conn := shortSetup(t)
	saveErr := errors.New("could not save servers to disk")
	exec = func(_ *ProxySQL, queryString string, _ ...interface{}) (sql.Result, error) {
		if queryString == "save mysql servers to disk" {
			return nil, saveErr
		}
		return nil, nil
	}
	err := conn.PersistChanges()
	if err != saveErr {
		t.Log("persist changes did not error on save failure")
		t.Fail()
	}
}

func TestPersistChangesErrorsOnLoad(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	defer resetHelpers()
	conn := longSetup(t)
	loadErr := errors.New("error saving servers to disk")
	exec = func(_ *ProxySQL, queryString string, _ ...interface{}) (sql.Result, error) {
		if queryString == "load mysql servers to runtime" {
			return nil, loadErr
		}
		return nil, nil
	}
	err := conn.PersistChanges()
	if err != loadErr {
		t.Log("persist changes did not error on load failure")
		t.Fail()
	}
}

func TestPersistChangesLoadsConfigurationToRuntime(t *testing.T) {
	defer SetupAndTeardownProxySQL(t)()
	conn := longSetup(t)
	// make entries map compare to runtime_servers.All()
	entries := []*Host{
		DefaultHost().SetHostname("reader1").SetHostgroupID(1),
		DefaultHost().SetHostname("reader2").SetHostgroupID(1),
		DefaultHost().SetHostname("writer").SetHostgroupID(0),
	}
	t.Log("inserting into ProxySQL")
	conn.AddHosts(entries...)
	err := conn.PersistChanges()
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
	conn := longSetup(t)
	var (
		port                = 3307
		hostname            = "mysql-1"
		max_connections     = 300
		hostgroup_id        = 1
		status              = "OFFLINE_SOFT"
		weight              = 2
		compression         = 1
		max_replication_lag = 1
		use_ssl             = 1
		max_latency_ms      = 1
		comment             = ":)"
	)
	err := conn.AddHost(Table("mysql_servers"), Port(port), Hostname(hostname), MaxConnections(max_connections), HostgroupID(hostgroup_id), Status(status), Weight(weight), Compression(compression), MaxReplicationLag(max_replication_lag), UseSSL(use_ssl), MaxLatencyMS(max_latency_ms), Comment(comment))
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

	if host.compression != compression {
		t.Fatalf("compression not set properly: %d", host.compression)
	}

	if host.max_replication_lag != max_replication_lag {
		t.Fatalf("max_replication_lag not set properly: %d", host.max_replication_lag)
	}

	if host.use_ssl != use_ssl {
		t.Fatalf("use_ssl not set properly: %d", host.use_ssl)
	}

	if host.max_latency_ms != max_latency_ms {
		t.Fatalf("max_latency_ms not set properly: %d", host.max_latency_ms)
	}

	if host.comment != comment {
		t.Fatalf("comment not set properly: %s", host.comment)
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
