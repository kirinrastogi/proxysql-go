package proxysql

// this file is for the host struct and functions on it

import (
	"fmt"
)

// Host represents a row in ProxySQL's mysql_servers config table
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

// DefaultHost returns a default host (in terms of the mysql_servers table).
// Note that hostname is left empty
func DefaultHost() *Host {
	return &Host{
		0,        // hostgroup_id
		"",       // hostname
		3306,     // port
		"ONLINE", // status
		1,        // weight
		0,        // compression
		1000,     // max_connections
		0,        // max_replication_lag
		0,        // use_ssl
		0,        // max_latency_ms
		"",       // comment
	}
}

// Setters for Host struct

func (h *Host) SetHostname(hn string) *Host {
	h.hostname = hn
	return h
}

func (h *Host) SetPort(p int) *Host {
	h.port = p
	return h
}

func (h *Host) SetHostgroupID(hg int) *Host {
	h.hostgroup_id = hg
	return h
}

func (h *Host) SetStatus(s string) *Host {
	h.status = s
	return h
}

func (h *Host) SetWeight(w int) *Host {
	h.weight = w
	return h
}

func (h *Host) SetCompression(c int) *Host {
	h.compression = c
	return h
}

func (h *Host) SetMaxConnections(m int) *Host {
	h.max_connections = m
	return h
}

func (h *Host) SetMaxReplicationLag(m int) *Host {
	h.max_replication_lag = m
	return h
}

func (h *Host) SetUseSSL(u int) *Host {
	h.use_ssl = u
	return h
}

func (h *Host) SetMaxLatencyMS(m int) *Host {
	h.max_latency_ms = m
	return h
}

func (h *Host) SetComment(c string) *Host {
	h.comment = c
	return h
}

// Builders for Host struct

func (h *Host) Hostname() string {
	return h.hostname
}

func (h *Host) Port() int {
	return h.port
}

func (h *Host) HostgroupID() int {
	return h.hostgroup_id
}

func (h *Host) Status() string {
	return h.status
}

func (h *Host) Weight() int {
	return h.weight
}

func (h *Host) Compression() int {
	return h.compression
}

func (h *Host) MaxConnections() int {
	return h.max_connections
}

func (h *Host) MaxReplicationLag() int {
	return h.max_replication_lag
}

func (h *Host) UseSSL() int {
	return h.use_ssl
}

func (h *Host) MaxLatencyMS() int {
	return h.max_latency_ms
}

func (h *Host) Comment() string {
	return h.comment
}

func (h *Host) Valid() error {
	hq := defaultHostQuery()
	hq.host = h
	return validateHostQuery(hq)
}

func (h *Host) values() string {
	return fmt.Sprintf("(%d, '%s', %d, '%s', %d, %d, %d, %d, %d, %d, '%s')", h.hostgroup_id, h.hostname, h.port, h.status, h.weight, h.compression, h.max_connections, h.max_replication_lag, h.use_ssl, h.max_latency_ms, h.comment)
}

func (h *Host) columns() string {
	return "(hostgroup_id, hostname, port, status, weight, compression, max_connections, max_replication_lag, use_ssl, max_latency_ms, comment)"
}

func (h *Host) where() string {
	return fmt.Sprintf("hostgroup_id = %d and hostname = '%s' and port = %d and status = '%s' and weight = %d and compression = %d and max_connections = %d and max_replication_lag = %d and use_ssl = %d and max_latency_ms = %d and comment = '%s'", h.hostgroup_id, h.hostname, h.port, h.status, h.weight, h.compression, h.max_connections, h.max_replication_lag, h.use_ssl, h.max_latency_ms, h.comment)
}
