package proxysql

import (
	"testing"
)

type hostQueryTests []struct {
	in  *hostQuery
	out error
}

func TestValidateTableOpts(t *testing.T) {
	tableTests := hostQueryTests{
		{defaultHostQuery(), nil},
		{defaultHostQuery().Table("runtime_mysql_servers"), nil},
		{defaultHostQuery().Table("bad table"), ErrConfigBadTable},
	}

	for _, testCase := range tableTests {
		obj := testCase.in
		err := testCase.out
		if validateTableOpts(obj) != err {
			t.Logf("did not match expected validation. table %s, err %v", obj.table, err)
			t.Fail()
		}
	}
}

func TestValidateHostgroup(t *testing.T) {
	hostgroupTests := hostQueryTests{
		{defaultHostQuery(), nil},
		{defaultHostQuery().Hostgroup(1), nil},
		{defaultHostQuery().Hostgroup(-1), ErrConfigBadHostgroup},
		{defaultHostQuery().Hostgroup(2147483649), ErrConfigBadHostgroup},
	}

	for _, testCase := range hostgroupTests {
		obj := testCase.in
		err := testCase.out
		if validateHostgroup(obj) != err {
			t.Logf("did not match expected validation. hg %d, err %v", obj.host.hostgroup_id, err)
			t.Fail()
		}
	}
}

func TestValidatePort(t *testing.T) {
	portTests := hostQueryTests{
		{defaultHostQuery(), nil},
		{defaultHostQuery().Port(0), nil},
		{defaultHostQuery().Port(65535), nil},
		{defaultHostQuery().Port(-1), ErrConfigBadPort},
		{defaultHostQuery().Port(65536), ErrConfigBadPort},
	}

	for _, testCase := range portTests {
		obj := testCase.in
		err := testCase.out
		if validatePort(obj) != err {
			t.Logf("did not match expected validation. port %d, err %v", obj.host.port, err)
			t.Fail()
		}
	}
}

func TestValidateQOpts(t *testing.T) {
	tests := hostQueryTests{
		{defaultHostQuery(), nil},
		{defaultHostQuery().Table("runtime_mysql_servers"), nil},
		{defaultHostQuery().Table("bad table").Port(123), ErrConfigBadTable},
		{defaultHostQuery().Hostgroup(1).Port(123).Table("runtime_mysql_servers"), nil},
		{defaultHostQuery().Hostgroup(-1), ErrConfigBadHostgroup},
		{defaultHostQuery().Hostgroup(2147483649), ErrConfigBadHostgroup},
		{defaultHostQuery().Port(0), nil},
		{defaultHostQuery().Port(65535), nil},
		{defaultHostQuery().Port(-1), ErrConfigBadPort},
		{defaultHostQuery().Port(65536), ErrConfigBadPort},
	}

	for _, testCase := range tests {
		obj := testCase.in
		err := testCase.out
		if validateHostQuery(obj) != err {
			t.Logf("did not match expected validation. obj %v, err %v", obj, err)
			t.Fail()
		}
	}
}

func TestDefaultHostQueryIsValid(t *testing.T) {
	if err := validateHostQuery(defaultHostQuery()); err != nil {
		t.Fatalf("default query object is not valid: %v", err)
	}
}

func TestEmptyHostQueryIsNotValid(t *testing.T) {
	if err := validateHostQuery(emptyHostQuery()); err == nil {
		t.Fatal("empty/invalid host query passed validation")
	}
}
