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

func TestValidateHostgroupID(t *testing.T) {
	hostgroupTests := hostQueryTests{
		{defaultHostQuery(), nil},
		{defaultHostQuery().HostgroupID(1), nil},
		{defaultHostQuery().HostgroupID(-1), ErrConfigBadHostgroupID},
		{defaultHostQuery().HostgroupID(2147483649), ErrConfigBadHostgroupID},
	}

	for _, testCase := range hostgroupTests {
		obj := testCase.in
		err := testCase.out
		if validateHostgroupID(obj) != err {
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

func TestValidateSpecifiedFields(t *testing.T) {
	specTests := hostQueryTests{
		{defaultHostQuery().HostgroupID(1).Port(1), nil},
		{defaultHostQuery().HostgroupID(1).Port(1).Port(2), ErrConfigDuplicateSpec},
	}

	for _, testCase := range specTests {
		obj := testCase.in
		err := testCase.out
		if validateSpecifiedFields(obj) != err {
			t.Logf("did not match expected validation. port %v, err %v", obj.specifiedFields, err)
			t.Fail()
		}
	}
}

func TestValidateHostname(t *testing.T) {
	hostnameTests := hostQueryTests{
		{defaultHostQuery(), ErrConfigNoHostname},
		{defaultHostQuery().Hostname("hostname"), nil},
	}

	for _, testCase := range hostnameTests {
		obj := testCase.in
		err := testCase.out
		if validateHostname(obj) != err {
			t.Logf("did not match expected validation. port %s, err %v", obj.host.hostname, err)
			t.Fail()
		}
	}
}

func TestValidateHostQuery(t *testing.T) {
	tests := hostQueryTests{
		{defaultHostQuery(), nil},
		{defaultHostQuery().Table("runtime_mysql_servers"), nil},
		{defaultHostQuery().Table("bad table").Port(123), ErrConfigBadTable},
		{defaultHostQuery().HostgroupID(1).Port(123).Table("runtime_mysql_servers"), nil},
		{defaultHostQuery().HostgroupID(-1), ErrConfigBadHostgroupID},
		{defaultHostQuery().HostgroupID(2147483649), ErrConfigBadHostgroupID},
		{defaultHostQuery().Port(0), nil},
		{defaultHostQuery().Port(65535), nil},
		{defaultHostQuery().Port(-1), ErrConfigBadPort},
		{defaultHostQuery().Port(65536), ErrConfigBadPort},
		{defaultHostQuery().MaxConnections(0), nil},
		{defaultHostQuery().MaxConnections(1), nil},
		{defaultHostQuery().MaxConnections(-1), ErrConfigBadMaxConnections},
		{defaultHostQuery().Status("ONLINE"), nil},
		{defaultHostQuery().Status("SHUNNED"), nil},
		{defaultHostQuery().Status("OFFLINE_SOFT"), nil},
		{defaultHostQuery().Status("OFFLINE_HARD"), nil},
		{defaultHostQuery().Status(""), ErrConfigBadStatus},
		{defaultHostQuery().Status("status"), ErrConfigBadStatus},
		{defaultHostQuery().Weight(0), nil},
		{defaultHostQuery().Weight(1), nil},
		{defaultHostQuery().Weight(-1), ErrConfigBadWeight},
		{defaultHostQuery().Compression(1), nil},
		{defaultHostQuery().Compression(-1), ErrConfigBadCompression},
		{defaultHostQuery().Compression(102401), ErrConfigBadCompression},
		{defaultHostQuery().MaxReplicationLag(1), nil},
		{defaultHostQuery().MaxReplicationLag(-1), ErrConfigBadMaxReplicationLag},
		{defaultHostQuery().MaxReplicationLag(126144001), ErrConfigBadMaxReplicationLag},
		{defaultHostQuery().UseSSL(0), nil},
		{defaultHostQuery().UseSSL(1), nil},
		{defaultHostQuery().UseSSL(2), ErrConfigBadUseSSL},
		{defaultHostQuery().MaxLatencyMS(0), nil},
		{defaultHostQuery().MaxLatencyMS(-1), ErrConfigBadMaxLatencyMS},
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
