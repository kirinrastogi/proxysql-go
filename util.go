package proxysql

// this file is for validation, and error specification

import (
	"errors"
)

type vOpts func(*hostQuery) error

var (
	ErrConfigBadTable             = errors.New("Bad table value, must be one of 'mysql_servers', 'runtime_mysql_servers'")
	ErrConfigBadHostgroupID       = errors.New("Bad hostgroup value, must be in [0, 2147483648]")
	ErrConfigBadPort              = errors.New("Bad port value, must be in [0, 65535]")
	ErrConfigBadMaxConnections    = errors.New("Bad max_connections value, must be > 0")
	ErrConfigBadStatus            = errors.New("Bad status value, must be one of 'ONLINE','SHUNNED','OFFLINE_SOFT', 'OFFLINE_HARD'")
	ErrConfigBadWeight            = errors.New("Bad weight value, must be > 0")
	ErrConfigBadCompression       = errors.New("Bad compression value, must be in [0, 102400]")
	ErrConfigBadMaxReplicationLag = errors.New("Bad max_replication_lag value, must be in [0, 126144000]")
	ErrConfigBadUseSSL            = errors.New("Bad use_ssl value, must be one of 0, 1")
	ErrConfigBadMaxLatencyMS      = errors.New("Bad max_latency_ms value, must be > 0")
	ErrConfigDuplicateSpec        = errors.New("Bad function call, a value was specified twice")
	ErrConfigNoHostname           = errors.New("Bad hostname, must not be empty")

	validationFuncs []vOpts
)

func init() {
	// add all validators to the validation array for validateHostQuery
	validationFuncs = append(validationFuncs, validateTableOpts)
	validationFuncs = append(validationFuncs, validateHostgroupID)
	validationFuncs = append(validationFuncs, validatePort)
	validationFuncs = append(validationFuncs, validateMaxConnections)
	validationFuncs = append(validationFuncs, validateStatus)
	validationFuncs = append(validationFuncs, validateWeight)
	validationFuncs = append(validationFuncs, validateCompression)
	validationFuncs = append(validationFuncs, validateMaxReplicationLag)
	validationFuncs = append(validationFuncs, validateUseSSL)
	validationFuncs = append(validationFuncs, validateMaxLatencyMS)
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

func validateHostgroupID(opts *hostQuery) error {
	if opts.host.hostgroup_id < 0 || opts.host.hostgroup_id > 2147483648 {
		return ErrConfigBadHostgroupID
	}
	return nil
}

func validatePort(opts *hostQuery) error {
	if opts.host.port < 0 || opts.host.port > 65535 {
		return ErrConfigBadPort
	}
	return nil
}

func validateMaxConnections(opts *hostQuery) error {
	if opts.host.max_connections < 0 {
		return ErrConfigBadMaxConnections
	}
	return nil
}

func validateStatus(opts *hostQuery) error {
	s := opts.host.status
	if s != "ONLINE" && s != "SHUNNED" && s != "OFFLINE_SOFT" && s != "OFFLINE_HARD" {
		return ErrConfigBadStatus
	}
	return nil
}

func validateWeight(opts *hostQuery) error {
	if opts.host.weight < 0 {
		return ErrConfigBadWeight
	}
	return nil
}

func validateCompression(opts *hostQuery) error {
	c := opts.host.compression
	if c < 0 || c > 102400 {
		return ErrConfigBadCompression
	}
	return nil
}

func validateMaxReplicationLag(opts *hostQuery) error {
	m := opts.host.max_replication_lag
	if m < 0 || m > 126144000 {
		return ErrConfigBadMaxReplicationLag
	}
	return nil
}

func validateUseSSL(opts *hostQuery) error {
	u := opts.host.use_ssl
	if u != 0 && u != 1 {
		return ErrConfigBadUseSSL
	}
	return nil
}

func validateMaxLatencyMS(opts *hostQuery) error {
	if opts.host.max_latency_ms < 0 {
		return ErrConfigBadMaxLatencyMS
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
