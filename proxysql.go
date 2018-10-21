package proxysql

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

type ProxySQL struct {
	dsn          string
	conn         *sql.DB
	defaultTable string
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

func (p *ProxySQL) Ping() error {
	return p.conn.Ping()
}

func (p *ProxySQL) Close() {
	p.conn.Close()
}

func (p *ProxySQL) Conn() *sql.DB {
	return p.conn
}

func (p *ProxySQL) SetDefaultTable(t string) error {
	if err := validateTable(t); err != nil {
		return fmt.Errorf("table %s is not one of mysql_servers, runtime_mysql_servers", t)
	}
	p.defaultTable = t
	return nil
}

func (p *ProxySQL) DefaultTable() string {
	return p.defaultTable
}

func (p *ProxySQL) PersistChanges() error {
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
	hostRows, err := p.conn.Query(fmt.Sprintf("select hostname from %s where hostname = '%s'", p.defaultTable, hostname))
	defer hostRows.Close()
	return hostRows.Next(), err
}

// Add host with values specified
// use default Host, and set with ...HostOpts

func (p *ProxySQL) AddHost(opts ...hostOpts) error {
	hostq, err := buildAndParseHostQueryWithHostname(p.defaultTable, opts...)
	if err != nil {
		return err
	}
	// build a query with these options
	_, err = exec(p, buildInsertQuery(hostq))
	return err
}

// Remove host with values specified
// like HostExists

func (p *ProxySQL) RemoveHost(hostname string) error {
	_, err := exec(p, fmt.Sprintf("delete from %s where hostname = '%s'", p.defaultTable, hostname))
	return err
}

// delete this

func (p *ProxySQL) RemoveHostFromHostgroup(hostname string, hostgroup int) error {
	_, err := p.conn.Exec(fmt.Sprintf("delete from %s where hostname = '%s' and hostgroup_id = %d", p.defaultTable, hostname, hostgroup))
	return err
}

// instead of string: int, it should be slice of Host s

func (p *ProxySQL) All() (map[string]int, error) {
	entries := make(map[string]int)
	allQuery := fmt.Sprintf("select hostname, hostgroup_id from %s", p.defaultTable)
	rows, err := query(p, allQuery)
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
		entries[hostname] = hostgroup
	}
	if rowsErr(rows) != nil && rowsErr(rows) != sql.ErrNoRows {
		return nil, rowsErr(rows)
	}
	return entries, nil
}

// maybe call this Like(), and return a slice of Host s that are Like the provided configuration

func (p *ProxySQL) Hostgroup(hostgroup int) (map[string]int, error) {
	entries := make(map[string]int)
	readQuery := fmt.Sprintf("select hostname, hostgroup_id from %s where hostgroup_id = %d", p.defaultTable, hostgroup)
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
	var numInstances int
	countQuery := fmt.Sprintf("select count(*) from %s where hostgroup_id = %d", p.defaultTable, hostgroup)
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
