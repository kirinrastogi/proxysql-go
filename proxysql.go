package proxysql

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"sync"
)

type ProxySQL struct {
	dsn  string
	conn *sql.DB
}

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

var mut sync.RWMutex

func (p *ProxySQL) Ping() error {
	return p.conn.Ping()
}

func (p *ProxySQL) Close() {
	p.conn.Close()
}

func (p *ProxySQL) Conn() *sql.DB {
	return p.conn
}

func (p *ProxySQL) PersistChanges() error {
	mut.Lock()
	defer mut.Unlock()
	_, err := exec(p, "save mysql servers to disk")
	if err != nil {
		return err
	}
	_, err = exec(p, "load mysql servers to runtime")
	if err != nil {
		return err
	}
	return nil
}

// HostExists with values specified ...HostOpts
// only include specified values in query
// if they want to delete a host with a specific hostname, only use that

func (p *ProxySQL) HostExists(hostname string) (bool, error) {
	mut.RLock()
	defer mut.RUnlock()
	hostRows, err := p.conn.Query(fmt.Sprintf("select hostname from mysql_servers where hostname = '%s'", hostname))
	defer hostRows.Close()
	return hostRows.Next(), err
}

// Add host with values specified
// use default Host, and set with ...HostOpts

func (p *ProxySQL) AddHost(opts ...hostOpts) error {
	hostq, err := buildAndParseHostQueryWithHostname(opts...)
	if err != nil {
		return err
	}
	// build a query with these options
	_, err = exec(p, buildInsertQuery(hostq))
	return err
}

// TODO func (p *ProxySQL) AddHosts(hosts ...Host) error {
// for when Hosts are built in memory, and then add all at once
// how to roll back?

// TODO func (p *ProxySQL) Clear() error {
// Convenience function, clears proxysql

// Remove host with values specified
// like HostExists

func (p *ProxySQL) RemoveHost(hostname string) error {
	mut.Lock()
	defer mut.Unlock()
	_, err := exec(p, fmt.Sprintf("delete from mysql_servers where hostname = '%s'", hostname))
	return err
}

// instead of string: int, it should be slice of Host s

func (p *ProxySQL) All() ([]Host, error) {
	mut.RLock()
	defer mut.RUnlock()
	entries := make([]Host, 0)
	allQuery := "select * from mysql_servers"
	rows, err := query(p, allQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			hostname            string
			hostgroup_id        int
			port                int
			status              string
			weight              int
			compression         int
			max_connections     int
			max_replication_lag int
			use_ssl             int
			max_latency_ms      int
			comment             string
		)
		err := scanRows(rows, &hostgroup_id, &hostname, &port, &status, &weight, &compression, &max_connections, &max_replication_lag, &use_ssl, &max_latency_ms, &comment)
		if err != nil {
			return nil, err
		}
		host := Host{hostname, hostgroup_id, port, status, weight, compression, max_connections, max_replication_lag, use_ssl, max_latency_ms, comment}
		append(entries, host)
	}
	if rowsErr(rows) != nil && rowsErr(rows) != sql.ErrNoRows {
		return nil, rowsErr(rows)
	}
	return entries, nil
}

// maybe call this Like(), and return a slice of Host s that are Like the provided configuration

func (p *ProxySQL) Hostgroup(hostgroup int) (map[string]int, error) {
	mut.RLock()
	defer mut.RUnlock()
	entries := make(map[string]int)
	readQuery := fmt.Sprintf("select hostname, hostgroup_id from mysql_servers where hostgroup_id = %d", hostgroup)
	rows, err := query(p, readQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			hostname  string
			hostgroup int
		)
		err := scanRows(rows, &hostname, &hostgroup)
		if err != nil {
			return nil, err
		}
		err = rowsErr(rows)
		if err != nil {
			return nil, err
		}
		entries[hostname] = hostgroup
	}
	return entries, nil
}

// call this AmountLike or something similar?
// and only query specifically with provided values

func (p *ProxySQL) SizeOfHostgroup(hostgroup int) (int, error) {
	mut.RLock()
	defer mut.RUnlock()
	var numInstances int
	countQuery := fmt.Sprintf("select count(*) from mysql_servers where hostgroup_id = %d", hostgroup)
	err := scanRow(p.conn.QueryRow(countQuery), &numInstances)
	if err != nil {
		return -1, err
	}
	return numInstances, nil
}

// TODO put these in an init() boi, so its easy to reset in tests
// wrappers around standard sql funcs for testing
var exec = func(p *ProxySQL, queryString string, _ ...interface{}) (sql.Result, error) {
	return p.conn.Exec(queryString)
}

var query = func(p *ProxySQL, queryString string, _ ...interface{}) (*sql.Rows, error) {
	return p.conn.Query(queryString)
}

var scanRows = func(rs *sql.Rows, dest ...interface{}) error {
	return rs.Scan(dest...)
}

var scanRow = func(rs *sql.Row, dest ...interface{}) error {
	return rs.Scan(dest...)
}

var rowsErr = func(rs *sql.Rows) error {
	return rs.Err()
}
