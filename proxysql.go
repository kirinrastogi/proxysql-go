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

// Add host with values specified
// use default Host, and set with ...HostOpts

func (p *ProxySQL) AddHost(opts ...hostOpts) error {
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

func (p *ProxySQL) AddHosts(hosts ...*Host) error {
	mut.Lock()
	defer mut.Unlock()
	for _, host := range hosts {
		insertQuery := fmt.Sprintf("insert into mysql_servers %s values %s", host.columns(), host.values())
		_, err := exec(p, insertQuery)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *ProxySQL) Clear() error {
	mut.Lock()
	defer mut.Unlock()
	_, err := exec(p, "delete from mysql_servers")
	return err
}

// Remove a host with values specified
// like AddHost
// it is recommended to only call this with RemoveHost(Hostname("a-hostname"))

func (p *ProxySQL) RemoveHost(opts ...hostOpts) error {
	mut.Lock()
	defer mut.Unlock()
	hostq, err := buildAndParseHostQuery(opts...)
	if err != nil {
		return err
	}
	// build a query with these options
	_, err = exec(p, buildDeleteQueryLimit(hostq))
	return err
}

// Remove all hosts that match the opts
func (p *ProxySQL) RemoveHostsLike(opts ...hostOpts) error {
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

// remove hosts that match these hosts exactly
func (p *ProxySQL) RemoveHosts(hosts ...*Host) error {
	mut.Lock()
	defer mut.Unlock()
	for _, host := range hosts {
		_, err := exec(p, fmt.Sprintf("delete from mysql_servers where %s", host.where()))
		if err != nil {
			return err
		}
	}
	return nil
}

// HostExists with values specified ...HostOpts
// only include specified values in query
// if they want to delete a host with a specific hostname, only use that

func (p *ProxySQL) HostsLike(opts ...hostOpts) ([]*Host, error) {
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
	return entries, nil
}

// instead of string: int, it should be slice of Host s

func (p *ProxySQL) All(opts ...hostOpts) ([]*Host, error) {
	// this is only used to get the table
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

var scanRow = func(rs *sql.Row, dest ...interface{}) error {
	return rs.Scan(dest...)
}

var rowsErr = func(rs *sql.Rows) error {
	return rs.Err()
}
