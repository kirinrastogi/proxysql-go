package proxysql

import (
	"fmt"
)

type Host struct {
	hostgroup_id        int
	hostname            string
	port                int
	status              string
	weight              int
	compression         int
	max_connections     int
	max_replication_lag int
	use_ssl             int
	max_latency_ms      int
	comment             string
}

func (h *Host) Hostname(hn string) *Host {
	h.hostname = hn
	return h
}

func (h *Host) Port(p int) *Host {
	h.port = p
	return h
}

func (h *Host) values() string {
	return fmt.Sprintf("(%d, '%s', %d, '%s', %d, %d, %d, %d, %d, %d, '%s')", h.hostgroup_id, h.hostname, h.port, h.status, h.weight, h.compression, h.max_connections, h.max_replication_lag, h.use_ssl, h.max_latency_ms, h.comment)
}

func (h *Host) columns() string {
	return "(hostgroup_id, hostname, port, status, weight, compression, max_connections, max_replication_lag, use_ssl, max_latency_ms, comment)"
}
