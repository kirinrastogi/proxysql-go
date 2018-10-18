package proxysql

import (
	"errors"
)

type vOpts func(*hostQuery) error

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

func validateTableOpts(opts *hostQuery) error {
	return validateTable(opts.table)
}

func validateTable(t string) error {
	if t != "mysql_servers" && t != "runtime_mysql_servers" {
		return ErrConfigBadTable
	}
	return nil
}

func validateHostgroup(opts *hostQuery) error {
	if opts.host.hostgroup_id < 0 || opts.host.hostgroup_id > 2147483648 {
		return ErrConfigBadHostgroup
	}
	return nil
}

func validatePort(opts *hostQuery) error {
	if opts.host.port < 0 || opts.host.port > 65535 {
		return ErrConfigBadPort
	}
	return nil
}

func validateHostQuery(opts *hostQuery) error {
	for _, validate := range validationFuncs {
		if err := validate(opts); err != nil {
			return err
		}
	}
	return nil
}
