package proxysql

import (
	"fmt"
)

type queryOpts struct {
	table     string
	hostgroup int
	port      int
}

type qOpts func(*queryOpts)

func Hostgroup(h int) qOpts {
	return func(opts *queryOpts) {
		opts.hostgroup = h
	}
}

func Table(t string) qOpts {
	return func(opts *queryOpts) {
		opts.table = t
	}
}

func Port(p int) qOpts {
	return func(opts *queryOpts) {
		opts.port = p
	}
}

func buildAndParseQueryOpts(setters ...qOpts) (*queryOpts, error) {
	opts := &queryOpts{
		table:     "mysql_servers",
		hostgroup: 0,
		port:      3306,
	}

	for _, setter := range setters {
		fmt.Printf("%v\n", setter)
		setter(opts)
	}

	// validate
	if err := validateQOpts(opts); err != nil {
		return nil, err
	}

	return opts, nil
}
