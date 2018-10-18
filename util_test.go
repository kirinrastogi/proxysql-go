package proxysql

import (
	"testing"
)

type queryOptsTests []struct {
	in  *queryOpts
	out error
}

func TestValidateTableOpts(t *testing.T) {
	tableTests := queryOptsTests{
		{defaultQueryOpts(), nil},
		{defaultQueryOpts().Table("runtime_mysql_servers"), nil},
		{defaultQueryOpts().Table("bad table"), ErrConfigBadTable},
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
	hostgroupTests := queryOptsTests{
		{defaultQueryOpts(), nil},
		{defaultQueryOpts().Hostgroup(1), nil},
		{defaultQueryOpts().Hostgroup(-1), ErrConfigBadHostgroup},
		{defaultQueryOpts().Hostgroup(2147483649), ErrConfigBadHostgroup},
	}

	for _, testCase := range hostgroupTests {
		obj := testCase.in
		err := testCase.out
		if validateHostgroup(obj) != err {
			t.Logf("did not match expected validation. hg %d, err %v", obj.hostgroup, err)
			t.Fail()
		}
	}
}

func TestValidatePort(t *testing.T) {
	portTests := queryOptsTests{
		{defaultQueryOpts(), nil},
		{defaultQueryOpts().Port(0), nil},
		{defaultQueryOpts().Port(65535), nil},
		{defaultQueryOpts().Port(-1), ErrConfigBadPort},
		{defaultQueryOpts().Port(65536), ErrConfigBadPort},
	}

	for _, testCase := range portTests {
		obj := testCase.in
		err := testCase.out
		if validatePort(obj) != err {
			t.Logf("did not match expected validation. port %d, err %v", obj.port, err)
			t.Fail()
		}
	}
}

func TestValidateQOpts(t *testing.T) {
	tests := queryOptsTests{
		{defaultQueryOpts(), nil},
		{defaultQueryOpts().Table("runtime_mysql_servers"), nil},
		{defaultQueryOpts().Table("bad table").Port(123), ErrConfigBadTable},
		{defaultQueryOpts().Hostgroup(1).Port(123).Table("runtime_mysql_servers"), nil},
		{defaultQueryOpts().Hostgroup(-1), ErrConfigBadHostgroup},
		{defaultQueryOpts().Hostgroup(2147483649), ErrConfigBadHostgroup},
		{defaultQueryOpts().Port(0), nil},
		{defaultQueryOpts().Port(65535), nil},
		{defaultQueryOpts().Port(-1), ErrConfigBadPort},
		{defaultQueryOpts().Port(65536), ErrConfigBadPort},
	}

	for _, testCase := range tests {
		obj := testCase.in
		err := testCase.out
		if validateQOpts(obj) != err {
			t.Logf("did not match expected validation. obj %v, err %v", obj, err)
			t.Fail()
		}
	}
}
