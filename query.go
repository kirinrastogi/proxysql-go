package proxysql

import (
	"fmt"
)

// TODO make this a struct that contains a Host, and a table ?
// Then hostOpts will set hostQuery.Host or hostQuery.table

type hostQuery struct {
	table string
	host  *Host
}

type hostOpts func(*hostQuery) *hostQuery

// because this is passed the defaults
func buildInsertQuery(opts *hostQuery) string {
	host := opts.host
	return fmt.Sprintf("insert into %s (hostgroup_id, hostname, port, status, weight, compression, max_connections, max_replication_lag, use_ssl, max_latency_ms, comment) values (%d, '%s', %d, '%s', %d, %d, %d, %d, %d, %d, '%s')", opts.table, host.hostgroup_id, host.hostname, host.port, host.status, host.weight, host.compression, host.max_connections, host.max_replication_lag, host.use_ssl, host.max_latency_ms, host.comment)
}

// TODO pass these slices that they modify??
// how do you determine what values to include in select queries?

func Hostgroup(h int) hostOpts {
	return func(opts *hostQuery) *hostQuery {
		return opts.Hostgroup(h)
	}
}

func Table(t string) hostOpts {
	return func(opts *hostQuery) *hostQuery {
		return opts.Table(t)
	}
}

func Port(p int) hostOpts {
	return func(opts *hostQuery) *hostQuery {
		return opts.Port(p)
	}
}

func (q *hostQuery) Table(t string) *hostQuery {
	q.table = t
	return q
}

func (q *hostQuery) Hostgroup(h int) *hostQuery {
	q.host.hostgroup_id = h
	return q
}

func (q *hostQuery) Port(p int) *hostQuery {
	q.host.port = p
	return q
}

// hostname is the only non default value
// if its not specified query should return an error

func defaultHost() *Host {
	return &Host{
		0,                // hostgroup_id
		"empty_hostname", // hostname
		3306,             // port
		"ONLINE",         // status
		1,                // weight
		0,                // compression
		1000,             // max_connections
		0,                // max_replication_lag
		0,                // use_ssl
		0,                // max_latency_ms
		"",               // comment
	}
}

func emptyHost() *Host {
	return &Host{
		-1,
		"empty_hostname",
		-1,
		"",
		-1,
		-1,
		-1,
		-1,
		-1,
		-1,
		"",
	}
}

// should have all zero values set
func defaultHostQuery() *hostQuery {
	return &hostQuery{
		table: "mysql_servers",
		host:  defaultHost(),
	}
}

// should have empty non valid values
// these need to be checked with a validation function
// when the query is being built
func emptyHostQuery() *hostQuery {
	return &hostQuery{
		table: "mysql_servers",
		host:  emptyHost(),
	}
}

func buildAndParseHostQuery(setters ...hostOpts) (*hostQuery, error) {
	opts := defaultHostQuery()
	for _, setter := range setters {
		setter(opts)
	}

	// validate
	if err := validateHostQuery(opts); err != nil {
		return nil, err
	}

	return opts, nil
}
