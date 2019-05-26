package main

import (
	"errors"
	. "github.com/kirinrastogi/proxysql-go"
	"log"
)

// For passing through the ProxySQLConn interface in tests
type ProxySQLMock struct {
	hosts   []*Host
	healthy bool
}

func NewProxySQLMock() *ProxySQLMock {
	return &ProxySQLMock{make([]*Host, 1), true}
}

// Mock ping func
func (p *ProxySQLMock) Ping() error {
	if !p.healthy {
		return errors.New("ping failed")
	}
	return nil
}

// Mock addhost and print opts
func (p *ProxySQLMock) AddHost(opts ...HostOpts) error {
	for _, opt := range opts {
		log.Println(opt)
	}
	return nil
}

// Mock addhosts and print them
func (p *ProxySQLMock) AddHosts(hosts ...*Host) error {
	for _, host := range hosts {
		log.Println(host)
	}
	return nil
}

// Mock saving, succeed if ping does
func (p *ProxySQLMock) PersistChanges() error {
	return p.Ping()
}
