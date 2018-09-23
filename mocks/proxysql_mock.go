package proxysql_mock

import (
	"database/sql"
	"fmt"
)

type ProxySQLMock struct {
	health                bool
	writerHostgroup       int
	readerHostgroup       int
	mysql_servers         map[string]int
	runtime_mysql_servers map[string]int
}

func New(writerHostgroup int, readerHostgroup int) *ProxySQLMock {
	pm := &ProxySQLMock{
		health:                true,
		writerHostgroup:       writerHostgroup,
		readerHostgroup:       readerHostgroup,
		mysql_servers:         make(map[string]int),
		runtime_mysql_servers: make(map[string]int),
	}
	return pm
}

func NewWithDefaultHostgroups() *ProxySQLMock {
	pm := &ProxySQLMock{
		health:                true,
		writerHostgroup:       0,
		readerHostgroup:       1,
		mysql_servers:         make(map[string]int),
		runtime_mysql_servers: make(map[string]int),
	}
	return pm
}

func (pm *ProxySQLMock) PersistChanges() error {
	for k, _ := range pm.runtime_mysql_servers {
		delete(pm.runtime_mysql_servers, k)
	}
	for k, v := range pm.mysql_servers {
		pm.runtime_mysql_servers[k] = v
	}
	return nil
}

func (pm *ProxySQLMock) Ping() error {
	if !pm.health {
		return fmt.Errorf("error reaching proxysql")
	}
	return nil
}

func (pm *ProxySQLMock) Writer() (string, error) {
	for hostname, hostgroup := range pm.mysql_servers {
		if hostgroup == 0 {
			return hostname, nil
		}
	}
	return "", sql.ErrNoRows
}

func (pm *ProxySQLMock) SetWriter(writer string, _ int) error {
	for hostname, hostgroup := range pm.mysql_servers {
		if hostgroup == 0 {
			delete(pm.mysql_servers, hostname)
			break
		}
	}
	pm.mysql_servers[writer] = 0
	return nil
}

func (pm *ProxySQLMock) HostExists(hostname string) (bool, error) {
	_, exists := pm.mysql_servers[hostname]
	return exists, nil
}

func (pm *ProxySQLMock) AddHost(hostname string, hostgroup int, _ int) error {
	for key, val := range pm.mysql_servers {
		if key == hostname && val == hostgroup {
			return fmt.Errorf("proxysql unique constraint hit")
		}
	}
	pm.mysql_servers[hostname] = hostgroup
	return nil
}

func (pm *ProxySQLMock) RemoveHost(hostname string) error {
	delete(pm.mysql_servers, hostname)
	return nil
}

func (pm *ProxySQLMock) RemoveHostFromHostgroup(hostname string, hostgroup int) error {
	for key, val := range pm.mysql_servers {
		if key == hostname && val == hostgroup {
			delete(pm.mysql_servers, key)
			return nil
		}
	}
	return nil
}

func (pm *ProxySQLMock) All() (map[string]int, error) {
	return pm.mysql_servers, nil
}

func (pm *ProxySQLMock) Hostgroup(hostgroup int) (map[string]int, error) {
	entries := make(map[string]int)
	for key, val := range pm.mysql_servers {
		if val == hostgroup {
			entries[key] = val
		}
	}
	return entries, nil
}

func (pm *ProxySQLMock) SizeOfHostgroup(hostgroup int) (int, error) {
	counter := 0
	for _, val := range pm.mysql_servers {
		if val == hostgroup {
			counter++
		}
	}
	return counter, nil
}
