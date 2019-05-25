package main

import (
	"errors"
	. "github.com/kirinrastogi/proxysql-go"
	"log"
)

type ProxySQLMock struct {
	hosts   []*Host
	healthy bool
}

func NewProxySQLMock() *ProxySQLMock {
	return &ProxySQLMock{make([]*Host, 1), true}
}

func (p *ProxySQLMock) Ping() error {
	if !p.healthy {
		return errors.New("ping failed")
	}
	return nil
}

func (p *ProxySQLMock) AddHost(opts ...HostOpts) error {
	for _, opt := range opts {
		log.Println(opt)
	}
	return nil
}

func (p *ProxySQLMock) AddHosts(hosts ...*Host) error {
	for _, host := range hosts {
		log.Println(host)
	}
	return nil
}

func (p *ProxySQLMock) PersistChanges() error {
	return p.Ping()
}
