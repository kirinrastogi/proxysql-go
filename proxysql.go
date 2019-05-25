package proxysql

// this file is for the functions on the ProxySQL struct

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

var mut sync.RWMutex

// Ping is a convenience function that calls the database/sql function on the
// underlying sql.DB connection
func (p *ProxySQL) Ping() error {
	return p.conn.Ping()
}

// Close is a convenience function that calls the database/sql function on the
// underlying sql.DB connection
func (p *ProxySQL) Close() {
	p.conn.Close()
}

// Conn is a convenience function that returns the underlying sql.DB connection
func (p *ProxySQL) Conn() *sql.DB {
	return p.conn
}

// PersistChanges saves the mysql servers config to disk, and then loads it
// to the runtime. This must be called for ProxySQL's changes to take effect
// This propogates errors from sql.Exec
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

// AddHost takes the configuration provided and inserts a host into ProxySQL
// with that configuration. This will return an error when a validation error
// of the configuration you specified occurs.
// This will propogate errors from sql.Exec as well
func (p *ProxySQL) AddHost(opts ...HostOpts) error {
	mut.Lock()
	defer mut.Unlock()
	hostq, err := buildAndParseHostQueryWithHostname(opts...)
	if err != nil {
		return err
	}
	// build a query with these options
	_, err = exec(p, buildInsertQuery(hostq))
	return err
}

// AddHosts will insert each of the hosts into mysql_servers
// this will error if any of the hosts are not valid
// this will propogate error from sql.Exec
func (p *ProxySQL) AddHosts(hosts ...*Host) error {
	mut.Lock()
	defer mut.Unlock()
	for _, host := range hosts {
		// TODO host validation to fail before it gets to ProxySQL
		insertQuery := fmt.Sprintf("insert into mysql_servers %s values %s", host.columns(), host.values())
		_, err := exec(p, insertQuery)
		if err != nil {
			return err
		}
	}
	return nil
}

// Clear is a convenience function to clear configuration
func (p *ProxySQL) Clear() error {
	mut.Lock()
	defer mut.Unlock()
	_, err := exec(p, "delete from mysql_servers")
	return err
}

// RemoveHost removes the host that matches the provided host's
// configuration exactly. This will propogate error from sql.Exec
func (p *ProxySQL) RemoveHost(host *Host) error {
	mut.Lock()
	defer mut.Unlock()
	// build a query with these options
	_, err := exec(p, fmt.Sprintf("delete from mysql_servers where %s", host.where()))
	return err
}

// RemoveHostsLike will remove all hosts that match the specified configuration
// This will error if configuration does not pass validation
// This will propogate error from sql.Exec
func (p *ProxySQL) RemoveHostsLike(opts ...HostOpts) error {
	mut.Lock()
	defer mut.Unlock()
	hostq, err := buildAndParseHostQuery(opts...)
	if err != nil {
		return err
	}
	// build a query with these options
	_, err = exec(p, buildDeleteQuery(hostq))
	return err
}

// RemoveHosts is a convenience function that removes hosts in the given slice
// This will propogate error from RemoveHost, or from sql.Exec
func (p *ProxySQL) RemoveHosts(hosts ...*Host) error {
	for _, host := range hosts {
		err := p.RemoveHost(host)
		if err != nil {
			return err
		}
	}
	return nil
}

// HostExists with values specified ...HostOpts
// only include specified values in query
// if they want to delete a host with a specific hostname, only use that

// HostsLike will return all hosts that match the given configuration
// This will error on configuration validation failing
// This will also propogate error from sql.Query, sql.Rows.Scan, sql.Rows.Err
func (p *ProxySQL) HostsLike(opts ...HostOpts) ([]*Host, error) {
	mut.RLock()
	defer mut.RUnlock()
	hostq, err := buildAndParseHostQuery(opts...)
	if err != nil {
		return nil, err
	}
	// run query built from these opts
	rows, err := query(p, buildSelectQuery(hostq))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	entries := make([]*Host, 0)
	for rows.Next() {
		var (
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
		)
		err := scanRows(rows, &hostgroup_id, &hostname, &port, &status, &weight, &compression, &max_connections, &max_replication_lag, &use_ssl, &max_latency_ms, &comment)
		if err != nil {
			return nil, err
		}
		host := &Host{hostgroup_id, hostname, port, status, weight, compression, max_connections, max_replication_lag, use_ssl, max_latency_ms, comment}
		entries = append(entries, host)
	}
	if rowsErr(rows) != nil && rowsErr(rows) != sql.ErrNoRows {
		return nil, rowsErr(rows)
	}
	return entries, nil
}

// All returns the state of the table that you specify
// This will error if configuration validation fails, you should only call
// this with All(Table("runtime_mysql_servers"))
// or just All() for "mysql_servers"
// This will also propogate error from sql.Query, sql.Rows.Scan, sql.Rows.Err
func (p *ProxySQL) All(opts ...HostOpts) ([]*Host, error) {
	// TODO: validation that only Table() was called
	hostq, err := buildAndParseHostQuery(opts...)
	if err != nil {
		return nil, err
	}
	mut.RLock()
	defer mut.RUnlock()
	entries := make([]*Host, 0)
	allQuery := fmt.Sprintf("select * from %s", hostq.table)
	rows, err := query(p, allQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
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
		)
		err := scanRows(rows, &hostgroup_id, &hostname, &port, &status, &weight, &compression, &max_connections, &max_replication_lag, &use_ssl, &max_latency_ms, &comment)
		if err != nil {
			return nil, err
		}
		host := &Host{hostgroup_id, hostname, port, status, weight, compression, max_connections, max_replication_lag, use_ssl, max_latency_ms, comment}
		entries = append(entries, host)
	}
	if rowsErr(rows) != nil && rowsErr(rows) != sql.ErrNoRows {
		return nil, rowsErr(rows)
	}
	return entries, nil
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

var rowsErr = func(rs *sql.Rows) error {
	return rs.Err()
}
