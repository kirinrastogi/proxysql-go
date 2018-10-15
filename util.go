package proxysql

import (
	"errors"
)

type vOpts func(*queryOpts) error

var (
	ErrConfigBadTable     error = errors.New("Bad table value, must be one of 'mysql_servers', 'runtime_mysql_servers'")
	ErrConfigBadHostgroup error = errors.New("Bad hostgroup value, must be in [0, 2147483648]")
	ErrConfigBadPort      error = errors.New("Bad port value, must be in [0, 65535]")

	validationFuncs []vOpts
)

func init() {
	// add all validators to the validation array
	validationFuncs = append(validationFuncs, validateTableOpts)
	validationFuncs = append(validationFuncs, validateHostgroup)
	validationFuncs = append(validationFuncs, validatePort)
}

func validateTableOpts(opts *queryOpts) error {
	if opts.table != "mysql_servers" && opts.table != "runtime_mysql_servers" {
		return ErrConfigBadTable
	}
	return nil
}

func validateTable(t string) error {
	if t != "mysql_servers" && t != "runtime_mysql_servers" {
		return ErrConfigBadTable
	}
	return nil
}

func validateHostgroup(opts *queryOpts) error {
	if opts.hostgroup < 0 || opts.hostgroup > 2147483648 {
		return ErrConfigBadHostgroup
	}
	return nil
}

func validatePort(opts *queryOpts) error {
	if opts.port < 0 || opts.port > 65535 {
		return ErrConfigBadPort
	}
	return nil
}

func validateQOpts(opts *queryOpts) error {
	for _, validate := range validationFuncs {
		if err := validate(opts); err != nil {
			return err
		}
	}
	return nil
}
