package proxysql

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

type ProxySQL struct {
	dsn             string
	conn            *sql.DB
	writerHostgroup int
	readerHostgroup int
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

func (p *ProxySQL) Writer() (string, error) {
	var writerHost string
	readQuery := fmt.Sprintf("select hostname from mysql_servers where hostgroup_id = %d", p.writerHostgroup)
	err := p.conn.QueryRow(readQuery).Scan(&writerHost)

	if err == sql.ErrNoRows {
		return "", err
	}
	return writerHost, nil
}

func (p *ProxySQL) SetWriter(hostname string, maxConnections int) error {
	writer, err := p.Writer()
	if err == sql.ErrNoRows && writer == "" {
		// if there is no writer, insert
		insertQuery := fmt.Sprintf("insert into mysql_servers (hostgroup_id, hostname, max_connections) values (%d, '%s', %d)", p.writerHostgroup, hostname, maxConnections)
		_, err = exec(p, insertQuery)
		if err != nil {
			return err
		}
	} else if err == nil {
		// writer exists, update
		updateQuery := fmt.Sprintf("update mysql_servers set hostname = '%s' where hostgroup_id = %d", hostname, p.writerHostgroup)
		_, err = exec(p, updateQuery)
		if err != nil {
			return err
		}
	}
	return err
}

func (p *ProxySQL) HostExists(hostname string) (bool, error) {
	hostRows, err := p.conn.Query(fmt.Sprintf("select hostname from mysql_servers where hostname = '%s'", hostname))
	defer hostRows.Close()
	return hostRows.Next(), err
}

func (p *ProxySQL) AddHost(hostname string, hostgroup int, maxConnections int) error {
	_, err := p.conn.Exec(fmt.Sprintf("insert into mysql_servers (hostgroup_id, hostname, max_connections) values (%d, '%s', %d)", hostgroup, hostname, maxConnections))
	return err
}

func (p *ProxySQL) RemoveHost(hostname string) error {
	_, err := p.conn.Exec(fmt.Sprintf("delete from mysql_servers where hostname = '%s'", hostname))
	return err
}

func (p *ProxySQL) RemoveHostFromHostgroup(hostname string, hostgroup int) error {
	_, err := p.conn.Exec(fmt.Sprintf("delete from mysql_servers where hostname = '%s' and hostgroup_id = %d", hostname, hostgroup))
	return err
}

func (p *ProxySQL) All() (map[string]int, error) {
	entries := make(map[string]int)
	rows, err := query(p, "select hostname, hostgroup_id from mysql_servers")
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
	rows, err := p.conn.Query(fmt.Sprintf("select hostname, hostgroup_id from mysql_servers where hostgroup_id = %d", hostgroup))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			hostname  string
			hostgroup int
		)
		err := rows.Scan(&hostname, &hostgroup)
		if err != nil {
			return nil, err
		}
		entries[hostname] = hostgroup
		if rows.Err() != nil && rows.Err() != sql.ErrNoRows {
			return nil, rows.Err()
		}
	}
	return entries, nil
}

func (p *ProxySQL) SizeOfHostgroup(hostgroup int) (int, error) {
	var numInstances int
	countQuery := fmt.Sprintf("select count(*) from mysql_servers where hostgroup_id = %d", hostgroup)
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
