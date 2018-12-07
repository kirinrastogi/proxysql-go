package proxysql

import (
	"testing"
)

func TestValues(t *testing.T) {
	h := DefaultHost().SetHostname("hn").SetPort(3307)
	s := h.values()
	t.Logf("string built: %s", s)
	if s != "(0, 'hn', 3307, 'ONLINE', 1, 0, 1000, 0, 0, 0, '')" {
		t.Fatalf("string from host.values was not expected: %s", s)
	}
}

func TestColumns(t *testing.T) {
	h := DefaultHost()
	s := h.columns()
	t.Logf("string built: %s", s)
	if s != "(hostgroup_id, hostname, port, status, weight, compression, max_connections, max_replication_lag, use_ssl, max_latency_ms, comment)" {
		t.Fatalf("string from host.columns was not expected: %s", s)
	}
}

func TestSetGetHostname(t *testing.T) {
	h := DefaultHost()
	ex := "a host"
	h.SetHostname(ex)
	if h.Hostname() != ex || h.hostname != ex {
		t.Fatalf("setter or getter for hostname broken: %s, %s, %s", h.Hostname(), ex, h.hostname)
	}
}

func TestSetGetPort(t *testing.T) {
	h := DefaultHost()
	ex := 3307
	h.SetPort(ex)
	if h.Port() != ex || h.port != ex {
		t.Fatalf("setter or getter for port broken: %d, %d, %d", h.Port(), ex, h.port)
	}
}

func TestSetGetHostgroupID(t *testing.T) {
	h := DefaultHost()
	ex := 2
	h.SetHostgroupID(ex)
	if h.HostgroupID() != ex || h.hostgroup_id != ex {
		t.Fatalf("setter or getter for hostgroup_id broken: %d, %d, %d", h.HostgroupID(), ex, h.hostgroup_id)
	}
}

func TestSetGetStatus(t *testing.T) {
	h := DefaultHost()
	ex := "OFFLINE_SOFT"
	h.SetStatus(ex)
	if h.Status() != ex || h.status != ex {
		t.Fatalf("setter or getter for status broken: %s, %s, %s", h.Status(), ex, h.status)
	}
}

func TestSetGetWeight(t *testing.T) {
	h := DefaultHost()
	ex := 2
	h.SetWeight(ex)
	if h.Weight() != ex || h.weight != ex {
		t.Fatalf("setter or getter for weight broken: %d, %d, %d", h.Weight(), ex, h.weight)
	}
}

func TestSetGetCompression(t *testing.T) {
	h := DefaultHost()
	ex := 10
	h.SetCompression(ex)
	if h.Compression() != ex || h.compression != ex {
		t.Fatalf("setter or getter for compression broken: %d, %d, %d", h.Compression(), ex, h.compression)
	}
}

func TestSetGetMaxConnections(t *testing.T) {
	h := DefaultHost()
	ex := 300
	h.SetMaxConnections(ex)
	if h.MaxConnections() != ex || h.max_connections != ex {
		t.Fatalf("setter or getter for max_connections broken: %d, %d, %d", h.MaxConnections(), ex, h.max_connections)
	}
}

func TestSetGetMaxReplicationLag(t *testing.T) {
	h := DefaultHost()
	ex := 900
	h.SetMaxReplicationLag(ex)
	if h.MaxReplicationLag() != ex || h.max_replication_lag != ex {
		t.Fatalf("setter or getter for max_replication_lag broken: %d, %d, %d", h.MaxReplicationLag(), ex, h.max_replication_lag)
	}
}

func TestSetGetUseSSL(t *testing.T) {
	h := DefaultHost()
	ex := 1
	h.SetUseSSL(ex)
	if h.UseSSL() != ex || h.use_ssl != ex {
		t.Fatalf("setter or getter for use_ssl broken: %d, %d, %d", h.UseSSL(), ex, h.use_ssl)
	}
}

func TestSetGetMaxLatencyMS(t *testing.T) {
	h := DefaultHost()
	ex := 900
	h.SetMaxLatencyMS(ex)
	if h.MaxLatencyMS() != ex || h.max_latency_ms != ex {
		t.Fatalf("setter or getter for max_latency_ms broken: %d, %d, %d", h.MaxLatencyMS(), ex, h.max_latency_ms)
	}
}

func TestSetGetComment(t *testing.T) {
	h := DefaultHost()
	ex := "a comment"
	h.SetComment(ex)
	if h.Comment() != ex || h.comment != ex {
		t.Fatalf("setter or getter for comment broken: %s, %s, %s", h.Comment(), ex, h.comment)
	}
}

func TestWhere(t *testing.T) {
	h := DefaultHost().SetHostname("hn").SetPort(3307).SetHostgroupID(1)
	s := h.where()
	t.Logf("string built: %s", s)
	if s != "hostgroup_id = 1 and hostname = 'hn' and port = 3307 and status = 'ONLINE' and weight = 1 and compression = 0 and max_connections = 1000 and max_replication_lag = 0 and use_ssl = 0 and max_latency_ms = 0 and comment = ''" {
		t.Fatalf("string from host.where was not expected: %s", s)
	}
}

func TestValid(t *testing.T) {
	if DefaultHost().SetHostname("hn").SetPort(-1).Valid() != ErrConfigBadPort {
		t.Fatal("host valid did not error expectedly")
	}
}
