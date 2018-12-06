package proxysql

import (
	"testing"
)

func TestValues(t *testing.T) {
	h := defaultHost().SetHostname("hn").SetPort(3307)
	s := h.values()
	t.Logf("string built: %s", s)
	if s != "(0, 'hn', 3307, 'ONLINE', 1, 0, 1000, 0, 0, 0, '')" {
		t.Fatalf("string from host.values was not expected: %s", s)
	}
}

func TestColumns(t *testing.T) {
	h := defaultHost()
	s := h.columns()
	t.Logf("string built: %s", s)
	if s != "(hostgroup_id, hostname, port, status, weight, compression, max_connections, max_replication_lag, use_ssl, max_latency_ms, comment)" {
		t.Fatalf("string from host.columns was not expected: %s", s)
	}
}

func TestWhere(t *testing.T) {
	h := defaultHost().SetHostname("hn").SetPort(3307).SetHostgroupID(1)
	s := h.where()
	t.Logf("string built: %s", s)
	if s != "hostgroup_id = 1 and hostname = 'hn' and port = 3307 and status = 'ONLINE' and weight = 1 and compression = 0 and max_connections = 1000 and max_replication_lag = 0 and use_ssl = 0 and max_latency_ms = 0 and comment = ''" {
		t.Fatalf("string from host.where was not expected: %s", s)
	}
}
