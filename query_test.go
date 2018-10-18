package proxysql

import (
	"reflect"
	"testing"
)

func TestTable(t *testing.T) {
	expected := "table"
	result := Table(expected)(defaultHostQuery()).table
	if result != expected {
		t.Fatalf("did not set table properly: %s", result)
	}
}

func TestHostgroup(t *testing.T) {
	expected := 1
	result := Hostgroup(expected)(defaultHostQuery()).host.hostgroup_id
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

func TestBuildAndParseHostQueryWithHostgroup(t *testing.T) {
	opts, err := buildAndParseHostQuery(Hostgroup(1))
	if err != nil {
		t.Logf("unexpected err: %v", err)
		t.Fail()
	}
	if !reflect.DeepEqual(opts, defaultHostQuery().Hostgroup(1)) {
		t.Fatalf("parsed opts were not default: %v", opts)
	}
}

func TestBuildAndParseHostQueryError(t *testing.T) {
	opts, err := buildAndParseHostQuery(Hostgroup(-1))
	if err != ErrConfigBadHostgroup {
		t.Logf("did not receive expected err: %v", err)
		t.Fail()
	}
	if opts != nil {
		t.Fatalf("returned query obj was not null: %v", opts)
	}
}
