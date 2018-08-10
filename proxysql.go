package proxysql

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

type ProxySQL struct {
	dsn  string
	conn *sql.DB
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
	_, err := p.conn.Exec("save mysql servers to disk")
	// return err here?
	// what if load fails but this doesn't?
	// maybe ping first?
	if err != nil {
		return err
	}
	_, err = p.conn.Exec("load mysql servers to runtime")
	if err != nil {
		return err
	}
	return nil
}

func (p *ProxySQL) Writer() (string, error) {
	var writerHost string
	err := p.conn.QueryRow("select hostname from mysql_servers where hostgroup_id = 0").Scan(&writerHost)

	if err == sql.ErrNoRows {
		return "", err
	}
	return writerHost, nil
}

func (p *ProxySQL) SetWriter(hostname string, maxConnections int) error {
	_, err := p.conn.Exec("delete from mysql_servers where hostgroup_id = 0")
	if err != nil {
		return err
	}
	_, err = p.conn.Exec(fmt.Sprintf("insert into mysql_servers (hostgroup_id, hostname, max_connections) values (%d, '%s', %d)", 0, hostname, maxConnections))
	if err != nil {
		return err
	}
	return nil
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
	rows, err := p.conn.Query("select hostname, hostgroup_id from mysql_servers")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		// scan multiple values?
		var hostname string
		var hostgroup int
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
	err := p.conn.QueryRow(fmt.Sprintf("select count(*) from mysql_servers where hostgroup_id = %d", hostgroup)).Scan(&numInstances)
	if err != nil {
		return -1, nil
	}
	return numInstances, nil
}
