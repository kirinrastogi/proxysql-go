package proxysql

import (
	"errors"
)

type vOpts func(*hostQuery) error

var (
	ErrConfigBadTable      error = errors.New("Bad table value, must be one of 'mysql_servers', 'runtime_mysql_servers'")
	ErrConfigBadHostgroup  error = errors.New("Bad hostgroup value, must be in [0, 2147483648]")
	ErrConfigBadPort       error = errors.New("Bad port value, must be in [0, 65535]")
	ErrConfigDuplicateSpec error = errors.New("Bad function call, a value was specified twice")
	ErrConfigNoHostname    error = errors.New("Bad hostname, must not be empty")

	validationFuncs []vOpts
)

func init() {
	// add all validators to the validation array for validateHostQuery
	validationFuncs = append(validationFuncs, validateTableOpts)
	validationFuncs = append(validationFuncs, validateHostgroup)
	validationFuncs = append(validationFuncs, validatePort)
	validationFuncs = append(validationFuncs, validateSpecifiedFields)
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

// returns ErrConfigDuplicateSpec if a duplicate occurs
func validateSpecifiedFields(opts *hostQuery) error {
	encountered := make(map[string]struct{})
	for _, field := range opts.specifiedFields {
		if _, exists := encountered[field]; exists {
			return ErrConfigDuplicateSpec
		}
		encountered[field] = struct{}{}
	}
	return nil
}

// This is called by functions that need a hostname
// it is not a default validation
func validateHostname(opts *hostQuery) error {
	if opts.host.hostname == "" {
		return ErrConfigNoHostname
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
