package proxysql

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

type ProxySQL struct {
	dsn             string
	conn            *sql.DB
	defaultTable    string
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

func (p *ProxySQL) HostExists(hostname string) (bool, error) {
	hostRows, err := p.conn.Query(fmt.Sprintf("select hostname from %s where hostname = '%s'", p.defaultTable, hostname))
	defer hostRows.Close()
	return hostRows.Next(), err
}

func (p *ProxySQL) AddHost(hostname string, hostgroup int, maxConnections int) error {
	_, err := p.conn.Exec(fmt.Sprintf("insert into %s (hostgroup_id, hostname, max_connections) values (%d, '%s', %d)", p.defaultTable, hostgroup, hostname, maxConnections))
	return err
}

func (p *ProxySQL) RemoveHost(hostname string) error {
	_, err := exec(p, fmt.Sprintf("delete from %s where hostname = '%s'", p.defaultTable, hostname))
	return err
}

func (p *ProxySQL) RemoveHostFromHostgroup(hostname string, hostgroup int) error {
	_, err := p.conn.Exec(fmt.Sprintf("delete from %s where hostname = '%s' and hostgroup_id = %d", p.defaultTable, hostname, hostgroup))
	return err
}

func (p *ProxySQL) All() (map[string]int, error) {
	entries := make(map[string]int)
	allQuery := fmt.Sprintf("select hostname, hostgroup_id from %s", p.defaultTable)
	rows, err := query(p, allQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var hostname string
		var hostgroup int
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

func (p *ProxySQL) SizeOfHostgroup(hostgroup int) (int, error) {
	var numInstances int
	countQuery := fmt.Sprintf("select count(*) from %s where hostgroup_id = %d", p.defaultTable, hostgroup)
	err := scanRow(p.conn.QueryRow(countQuery), &numInstances)
	if err != nil {
		return -1, err
	}
	return numInstances, nil
}

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
