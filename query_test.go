package proxysql

import (
	"reflect"
	"testing"
)

func TestTable(t *testing.T) {
	expected := "table"
	result := Table(expected)(defaultQueryOpts()).table
	if result != expected {
		t.Fatalf("did not set table properly: %s", result)
	}
}

func TestHostgroup(t *testing.T) {
	expected := 1
	result := Hostgroup(expected)(defaultQueryOpts()).hostgroup
	if result != expected {
		t.Fatalf("did not set hostgroup properly: %d", result)
	}
}

func TestPort(t *testing.T) {
	expected := 3305
	result := Port(expected)(defaultQueryOpts()).port
	if result != expected {
		t.Fatalf("did not set port properly: %d", result)
	}
}

func TestBuildAndParseEmptyQueryOpts(t *testing.T) {
	opts, err := buildAndParseQueryOpts()
	if err != nil {
		t.Logf("unexpected err: %v", err)
		t.Fail()
	}
	if !reflect.DeepEqual(opts, defaultQueryOpts()) {
		t.Fatalf("parsed opts were not default: %v", opts)
	}
}

func TestBuildAndParseQueryOptsWithHostgroup(t *testing.T) {
	opts, err := buildAndParseQueryOpts(Hostgroup(1))
	if err != nil {
		t.Logf("unexpected err: %v", err)
		t.Fail()
	}
	if !reflect.DeepEqual(opts, defaultQueryOpts().Hostgroup(1)) {
		t.Fatalf("parsed opts were not default: %v", opts)
	}
}

func TestBuildAndParseQueryOptsError(t *testing.T) {
	opts, err := buildAndParseQueryOpts(Hostgroup(-1))
	if err != ErrConfigBadHostgroup {
		t.Logf("did not receive expected err: %v", err)
		t.Fail()
	}
	if opts != nil {
		t.Fatalf("returned query obj was not null: %v", opts)
	}
}
