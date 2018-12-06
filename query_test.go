package proxysql

import (
	"reflect"
	"strings"
	"testing"
)

func TestTable(t *testing.T) {
	expected := "table"
	result := Table(expected)(defaultHostQuery()).table
	if result != expected {
		t.Fatalf("did not set table properly: %s", result)
	}
}

func TestHostgroupID(t *testing.T) {
	expected := 1
	result := HostgroupID(expected)(defaultHostQuery()).host.hostgroup_id
	if result != expected {
		t.Fatalf("did not set hostgroup properly: %d", result)
	}
}

func TestPort(t *testing.T) {
	expected := 3305
	result := Port(expected)(defaultHostQuery()).host.port
	if result != expected {
		t.Fatalf("did not set port properly: %d", result)
	}
}

func TestHostname(t *testing.T) {
	expected := "hostname"
	result := Hostname(expected)(defaultHostQuery()).host.hostname
	if result != expected {
		t.Fatalf("did not set hostname properly: %s", result)
	}
}

func TestMaxConnections(t *testing.T) {
	expected := 300
	result := MaxConnections(expected)(defaultHostQuery()).host.max_connections
	if result != expected {
		t.Fatalf("did not set max_connections properly: %d", result)
	}
}

func TestStatus(t *testing.T) {
	expected := "OFFLINE_SOFT"
	result := Status(expected)(defaultHostQuery()).host.status
	if result != expected {
		t.Fatalf("did not set status properly: %s", result)
	}
}

func TestWeight(t *testing.T) {
	expected := 2
	result := Weight(expected)(defaultHostQuery()).host.weight
	if result != expected {
		t.Fatalf("did not set weight properly: %d", result)
	}
}

func TestCompression(t *testing.T) {
	expected := 1
	result := Compression(expected)(defaultHostQuery()).host.compression
	if result != expected {
		t.Fatalf("did not set compression properly: %d", result)
	}
}

func TestMaxReplicationLag(t *testing.T) {
	expected := 1
	result := MaxReplicationLag(expected)(defaultHostQuery()).host.max_replication_lag
	if result != expected {
		t.Fatalf("did not set max_replication_lag properly: %d", result)
	}
}

func TestUseSSL(t *testing.T) {
	expected := 1
	result := UseSSL(expected)(defaultHostQuery()).host.use_ssl
	if result != expected {
		t.Fatalf("did not set use_ssl properly: %d", result)
	}
}

func TestMaxLatencyMS(t *testing.T) {
	expected := 1
	result := MaxLatencyMS(expected)(defaultHostQuery()).host.max_latency_ms
	if result != expected {
		t.Fatalf("did not set max_latency_ms properly: %d", result)
	}
}

func TestComment(t *testing.T) {
	expected := "comment"
	result := Comment(expected)(defaultHostQuery()).host.comment
	if result != expected {
		t.Fatalf("did not set comment properly: %s", result)
	}
}

func TestBuildAndParseEmptyHostQuery(t *testing.T) {
	opts, err := buildAndParseHostQuery()
	if err != nil {
		t.Logf("unexpected err: %v", err)
		t.Fail()
	}
	if !reflect.DeepEqual(opts, defaultHostQuery()) {
		t.Fatalf("parsed opts were not default: %v", opts)
	}
}

func TestBuildAndParseEmptyHostQueryWithHostnameFailsHostnameValidation(t *testing.T) {
	opts, err := buildAndParseHostQueryWithHostname()
	if err != ErrConfigNoHostname {
		t.Logf("did not get expected err: %v", err)
		t.Fail()
	}
	if opts != nil {
		t.Fatalf("did not receive non nil opts: %v", opts)
	}
}

func TestBuildAndParseHostQueryWithHostnameFailsWhenParentFails(t *testing.T) {
	opts, err := buildAndParseHostQueryWithHostname(Port(1), Port(2))
	if err != ErrConfigDuplicateSpec {
		t.Logf("did not get expected err: %v", err)
		t.Fail()
	}
	if opts != nil {
		t.Fatalf("did not receive non nil opts: %v", opts)
	}
}

func TestBuildAndParseHostQueryWithHostnameSucceedsWithHostname(t *testing.T) {
	opts, err := buildAndParseHostQueryWithHostname(Hostname("hostname"))
	if err != nil {
		t.Logf("unexpected err: %v", err)
		t.Fail()
	}
	if !reflect.DeepEqual(opts, defaultHostQuery().Hostname("hostname")) {
		t.Fatalf("parsed opts were not default: %v", opts)
	}
}

func TestBuildAndParseHostQueryWithHostgroupID(t *testing.T) {
	opts, err := buildAndParseHostQuery(HostgroupID(1))
	if err != nil {
		t.Logf("unexpected err: %v", err)
		t.Fail()
	}
	if !reflect.DeepEqual(opts, defaultHostQuery().HostgroupID(1)) {
		t.Fatalf("parsed opts were not default: %v", opts)
	}
}

func TestBuildAndParseHostQueryError(t *testing.T) {
	opts, err := buildAndParseHostQuery(HostgroupID(-1))
	if err != ErrConfigBadHostgroupID {
		t.Logf("did not receive expected err: %v", err)
		t.Fail()
	}
	if opts != nil {
		t.Fatalf("returned query obj was not null: %v", opts)
	}
}

func TestBuildSpecifiedColumns(t *testing.T) {
	opts, err := buildAndParseHostQuery(HostgroupID(1), Port(12))
	if err != nil {
		t.Logf("unexpected parse error: %v", err)
		t.Fail()
	}

	queryString := buildSpecifiedColumns(opts.specifiedFields)
	expected := "(hostgroup_id, port)"
	if expected != queryString {
		t.Fatalf("specified fields returned were not expected: %s != %s", expected, queryString)
	}
}

func TestBuildSpecifiedColumnsDifferentOrder(t *testing.T) {
	opts, err := buildAndParseHostQuery(Port(1), HostgroupID(12))
	if err != nil {
		t.Logf("unexpected parse error: %v", err)
		t.Fail()
	}

	queryString := buildSpecifiedColumns(opts.specifiedFields)
	expected := "(port, hostgroup_id)"
	if expected != queryString {
		t.Fatalf("specified fields returned were not expected: %s != %s", expected, queryString)
	}
}

func TestBuildSpecifiedColumnsDoesntGiveUsDuplicates(t *testing.T) {
	opts, err := buildAndParseHostQuery(Port(1), HostgroupID(12), Port(2))
	if err != ErrConfigDuplicateSpec {
		t.Logf("unexpected parse error: %v", err)
		t.Fail()
	}

	if opts != nil {
		t.Fatalf("did not receive nil value: %v", opts)
	}
}

func TestBuildSpecifiedColumnsIsOrderDependent(t *testing.T) {
	opts, err := buildAndParseHostQuery(Port(1), HostgroupID(12))
	if err != nil {
		t.Logf("unexpected parse error: %v", err)
		t.Fail()
	}

	for i := 0; i < 10; i++ {
		queryString := buildSpecifiedColumns(opts.specifiedFields)
		expected := "(port, hostgroup_id)"
		if expected != queryString {
			t.Fatalf("specified fields returned were not expected: %s != %s", expected, queryString)
		}
	}
}

func TestBuildSpecifiedValues(t *testing.T) {
	opts, err := buildAndParseHostQuery(Port(1), HostgroupID(3), Table("runtime_mysql_servers"), Hostname("host"))
	if err != nil {
		t.Logf("unexpected parse error: %v", err)
		t.Fail()
	}

	queryString := buildSpecifiedValues(opts)
	expected := "(1, 3, 'host')"
	if expected != queryString {
		t.Fatalf("did not receive expected string, %s != %s", expected, queryString)
	}
}

func TestColumnsAndValuesHaveTheSameAmountOfCommas(t *testing.T) {
	opts, err := buildAndParseHostQuery(Port(1), HostgroupID(3), Table("runtime_mysql_servers"), Hostname("host"))
	if err != nil {
		t.Logf("unexpected parse error: %v", err)
		t.Fail()
	}

	values := buildSpecifiedValues(opts)
	columns := buildSpecifiedColumns(opts.specifiedFields)
	if len(strings.Split(values, " ")) != len(strings.Split(columns, " ")) {
		t.Fatalf("val and cols for query building not same size: %s, %s", values, columns)
	}
}

func TestBuildInsertQuery(t *testing.T) {
	opts, err := buildAndParseHostQuery(Port(1), HostgroupID(3), Table("runtime_mysql_servers"), Hostname("host"))
	if err != nil {
		t.Logf("unexpected parse error: %v", err)
		t.Fail()
	}

	q := buildInsertQuery(opts)
	t.Logf("query: %s", q)
	if !strings.Contains(q, "runtime_mysql_servers") {
		t.Log("insert query built did use specified table runtime_mysql_servers")
		t.Fail()
	}

	for _, field := range opts.specifiedFields {
		if !strings.Contains(q, field) {
			t.Logf("insert query built did not contain field %s", field)
			t.Fail()
		}
	}
}

func TestBuildSelectQuery(t *testing.T) {
	opts, err := buildAndParseHostQuery(Port(1), HostgroupID(3), Table("runtime_mysql_servers"), Hostname("host"))
	if err != nil {
		t.Logf("unexpected parse error: %v", err)
		t.Fail()
	}

	q := buildSelectQuery(opts)
	t.Logf("query: %s", q)
	if !strings.Contains(q, "runtime_mysql_servers") {
		t.Log("select query built did use specified table runtime_mysql_servers")
		t.Fail()
	}

	for _, field := range opts.specifiedFields {
		if !strings.Contains(q, field) {
			t.Logf("select query built did not contain field %s", field)
			t.Fail()
		}
	}
}

func TestBuildDeleteQueryLimit(t *testing.T) {
	opts, err := buildAndParseHostQuery(Port(1), HostgroupID(3), Hostname("host"))
	if err != nil {
		t.Logf("unexpected parse error: %v", err)
		t.Fail()
	}

	q := buildDeleteQuery(opts)
	t.Logf("query: %s", q)
	if !strings.Contains(q, "mysql_servers") {
		t.Log("select query built did use default table mysql_servers")
		t.Fail()
	}

	for _, field := range opts.specifiedFields {
		if !strings.Contains(q, field) {
			t.Logf("select query built did not contain field %s", field)
			t.Fail()
		}
	}
}
