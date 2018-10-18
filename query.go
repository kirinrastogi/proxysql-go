package proxysql

// TODO make this a struct that contains a Host, and a table ?
// Then qOpts will set queryOpts.Host or queryOpts.table

type queryOpts struct {
	table     string
	hostgroup int
	port      int
}

type qOpts func(*queryOpts) *queryOpts

func Hostgroup(h int) qOpts {
	return func(opts *queryOpts) *queryOpts {
		return opts.Hostgroup(h)
	}
}

func Table(t string) qOpts {
	return func(opts *queryOpts) *queryOpts {
		return opts.Table(t)
	}
}

func Port(p int) qOpts {
	return func(opts *queryOpts) *queryOpts {
		return opts.Port(p)
	}
}

func (q *queryOpts) Table(t string) *queryOpts {
	q.table = t
	return q
}

func (q *queryOpts) Hostgroup(h int) *queryOpts {
	q.hostgroup = h
	return q
}

func (q *queryOpts) Port(p int) *queryOpts {
	q.port = p
	return q
}

// TODO move this to util.go
// TODO add defaultNoOpQueryOpts() for only including certain data in sql query generation

func defaultQueryOpts() *queryOpts {
	return &queryOpts{
		table:     "mysql_servers",
		hostgroup: 0,
		port:      3306,
	}
}

func buildAndParseQueryOpts(setters ...qOpts) (*queryOpts, error) {
	opts := defaultQueryOpts()
	for _, setter := range setters {
		setter(opts)
	}

	// validate
	if err := validateQOpts(opts); err != nil {
		return nil, err
	}

	return opts, nil
}
