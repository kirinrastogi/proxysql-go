package proxysql

import (
	"bytes"
	"fmt"
)

// TODO make this a struct that contains a Host, and a table ?
// Then hostOpts will set hostQuery.Host or hostQuery.table

type hostQuery struct {
	table string
	host  *Host
	// TODO make this a map[string]struct{} so no duplicates
	specifiedFields []string
}

type hostOpts func(*hostQuery) *hostQuery

// reads
// select (specified_values) from table where a=b c='d' e=f

// writes
// delete from TABLE where a=b c='d' e=f
// insert into TABLE (specified_values) values (b, 'd', f)

// given a hostQuery
// returns a string like (sv_1, sv_2, sv_3)
func buildSpecifiedColumns(opts *hostQuery) string {
	var buffer bytes.Buffer
	for pos, col := range opts.specifiedFields {
		buffer.WriteString(col)
		// don't add a comma at the end if its last one
		if pos != len(opts.specifiedFields)-1 {
			buffer.WriteString(", ")
		}
	}
	return fmt.Sprintf("(%s)", buffer.String())
}

// because this is passed the defaults
func buildInsertQuery(opts *hostQuery) string {
	host := opts.host
	// query is concat of base + specs
	return fmt.Sprintf("insert into %s (hostgroup_id, hostname, port, status, weight, compression, max_connections, max_replication_lag, use_ssl, max_latency_ms, comment) values (%d, '%s', %d, '%s', %d, %d, %d, %d, %d, %d, '%s')", opts.table, host.hostgroup_id, host.hostname, host.port, host.status, host.weight, host.compression, host.max_connections, host.max_replication_lag, host.use_ssl, host.max_latency_ms, host.comment)
}

// use this when building queries, include the value if it is specified.
// if this is the table, use that too, as the specified table trumps the default one
func (opts *hostQuery) specifyField(field string) *hostQuery {
	opts.specifiedFields = append(opts.specifiedFields, field)
	return opts
}

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
	return q.specifyField("table")
}

func (q *hostQuery) Hostgroup(h int) *hostQuery {
	q.host.hostgroup_id = h
	return q.specifyField("hostgroup_id")
}

func (q *hostQuery) Port(p int) *hostQuery {
	q.host.port = p
	return q.specifyField("port")
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

// should have all zero values set
func defaultHostQuery() *hostQuery {
	return &hostQuery{
		table: "mysql_servers",
		host:  defaultHost(),
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
