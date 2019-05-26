package proxysql

// this file is for query generation, and configuration

import (
	"bytes"
	"fmt"
	"reflect"
)

// the type of queries we want to build are as follows:

// select (specified_values) from table where a=b c='d' e=f

// delete from TABLE where a=b c='d' e=f

// insert into TABLE (specified_values) values (b, 'd', f)

type hostQuery struct {
	table           string
	host            *Host
	specifiedFields []string
}

// HostOpts is a type of function that is called with a hostQuery struct to
// specify a value in a query
type HostOpts func(*hostQuery) *hostQuery

// given a slice of fields
// returns a string like (sv_1, sv_2, sv_3)
func buildSpecifiedColumns(specifiedFields []string) string {
	var buffer bytes.Buffer
	for pos, field := range specifiedFields {
		buffer.WriteString(field)
		// don't add a comma at the end if its last one
		if pos != len(specifiedFields)-1 {
			buffer.WriteString(", ")
		}
	}
	return fmt.Sprintf("(%s)", buffer.String())
}

func valueAsString(opts *hostQuery, field string) string {
	var stringValue string
	r := reflect.ValueOf(opts.host)
	val := reflect.Indirect(r).FieldByName(field)
	if val.Type().Name() == "int" {
		stringValue = fmt.Sprintf("%v", val)
	} else if val.Type().Name() == "string" {
		stringValue = fmt.Sprintf("'%s'", val.String())
	}
	return stringValue
}

// given a host and specifiedFields it builds a string like
// (b, 'd', f)
// where b, d, and f are values of type int, string, int
func buildSpecifiedValues(opts *hostQuery) string {
	var buffer bytes.Buffer
	for pos, field := range opts.specifiedFields {
		buffer.WriteString(valueAsString(opts, field))
		if pos != len(opts.specifiedFields)-1 {
			buffer.WriteString(", ")
		}
	}
	return fmt.Sprintf("(%s)", buffer.String())
}

// given a host and specifiedFields, build a string like
// a1 = b1 and a2 = b2
func buildSpecifiedValuesWhere(opts *hostQuery) string {
	var buffer bytes.Buffer
	for pos, field := range opts.specifiedFields {
		buffer.WriteString(fmt.Sprintf("%s = ", field))
		buffer.WriteString(valueAsString(opts, field))
		if pos != len(opts.specifiedFields)-1 {
			buffer.WriteString(" and ")
		}
	}
	return buffer.String()
}

func buildInsertQuery(opts *hostQuery) string {
	return fmt.Sprintf("insert into %s %s values %s", opts.table, buildSpecifiedColumns(opts.specifiedFields), buildSpecifiedValues(opts))
}

// builds a select query that only takes in to account the specified columns
func buildSelectQuery(opts *hostQuery) string {
	return fmt.Sprintf("select * from %s where %s", opts.table, buildSpecifiedValuesWhere(opts))
}

// builds a delete query
func buildDeleteQuery(opts *hostQuery) string {
	return fmt.Sprintf("delete from %s where %s", opts.table, buildSpecifiedValuesWhere(opts))
}

// use this when building queries, include the value if it is specified.
// if this is the table, use that too, as the specified table trumps the default one
func (opts *hostQuery) specifyField(field string) *hostQuery {
	opts.specifiedFields = append(opts.specifiedFields, field)
	return opts
}

// HostgroupID sets the 'hostgroup_id' in a query
func HostgroupID(h int) HostOpts {
	return func(opts *hostQuery) *hostQuery {
		return opts.HostgroupID(h)
	}
}

// Table sets the table in a query
// One of 'runtime_mysql_servers' or 'mysql_servers'
func Table(t string) HostOpts {
	return func(opts *hostQuery) *hostQuery {
		return opts.Table(t)
	}
}

// Port sets the 'port' in a query
func Port(p int) HostOpts {
	return func(opts *hostQuery) *hostQuery {
		return opts.Port(p)
	}
}

// Hostname sets the 'hostname' in a query
func Hostname(h string) HostOpts {
	return func(opts *hostQuery) *hostQuery {
		return opts.Hostname(h)
	}
}

// MaxConnections sets the 'max_connections' in a query
func MaxConnections(c int) HostOpts {
	return func(opts *hostQuery) *hostQuery {
		return opts.MaxConnections(c)
	}
}

// Status sets the 'status' in a query
func Status(s string) HostOpts {
	return func(opts *hostQuery) *hostQuery {
		return opts.Status(s)
	}
}

// Weight sets the 'weight' in a query
func Weight(w int) HostOpts {
	return func(opts *hostQuery) *hostQuery {
		return opts.Weight(w)
	}
}

// Compression sets the 'compression' in a query
func Compression(c int) HostOpts {
	return func(opts *hostQuery) *hostQuery {
		return opts.Compression(c)
	}
}

// MaxReplicationLag sets the 'max_replication_lag' in a query
func MaxReplicationLag(m int) HostOpts {
	return func(opts *hostQuery) *hostQuery {
		return opts.MaxReplicationLag(m)
	}
}

// UseSSL sets the 'use_ssl' in a query
func UseSSL(u int) HostOpts {
	return func(opts *hostQuery) *hostQuery {
		return opts.UseSSL(u)
	}
}

// MaxLatencyMS sets the 'max_latency_ms' in a query
func MaxLatencyMS(m int) HostOpts {
	return func(opts *hostQuery) *hostQuery {
		return opts.MaxLatencyMS(m)
	}
}

// Comment sets the 'comment' in a query
func Comment(c string) HostOpts {
	return func(opts *hostQuery) *hostQuery {
		return opts.Comment(c)
	}
}

func (opts *hostQuery) Table(t string) *hostQuery {
	// don't put it in specifyFields because that slice is used for
	// building wheres and values
	opts.table = t
	return opts
}

func (opts *hostQuery) HostgroupID(h int) *hostQuery {
	opts.host.hostgroup_id = h
	return opts.specifyField("hostgroup_id")
}

func (opts *hostQuery) Port(p int) *hostQuery {
	opts.host.port = p
	return opts.specifyField("port")
}

func (opts *hostQuery) Hostname(h string) *hostQuery {
	opts.host.hostname = h
	return opts.specifyField("hostname")
}

func (opts *hostQuery) MaxConnections(c int) *hostQuery {
	opts.host.max_connections = c
	return opts.specifyField("max_connections")
}

func (opts *hostQuery) Status(s string) *hostQuery {
	opts.host.status = s
	return opts.specifyField("status")
}

func (opts *hostQuery) Weight(w int) *hostQuery {
	opts.host.weight = w
	return opts.specifyField("weight")
}

func (opts *hostQuery) Compression(c int) *hostQuery {
	opts.host.compression = c
	return opts.specifyField("compression")
}

func (opts *hostQuery) MaxReplicationLag(m int) *hostQuery {
	opts.host.max_replication_lag = m
	return opts.specifyField("max_replication_lag")
}

func (opts *hostQuery) UseSSL(u int) *hostQuery {
	opts.host.use_ssl = u
	return opts.specifyField("use_ssl")
}

func (opts *hostQuery) MaxLatencyMS(m int) *hostQuery {
	opts.host.max_latency_ms = m
	return opts.specifyField("max_latency_ms")
}

func (opts *hostQuery) Comment(c string) *hostQuery {
	opts.host.comment = c
	return opts.specifyField("comment")
}

// should have all zero values set
func defaultHostQuery() *hostQuery {
	return &hostQuery{
		table: "mysql_servers",
		host:  DefaultHost(),
	}
}

func buildAndParseHostQuery(setters ...HostOpts) (*hostQuery, error) {
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

// same as above but mandatory hostname
func buildAndParseHostQueryWithHostname(setters ...HostOpts) (*hostQuery, error) {
	opts, err := buildAndParseHostQuery(setters...)
	if err != nil {
		return nil, err
	}

	if err = validateHostname(opts); err != nil {
		return nil, err
	}
	return opts, nil
}
