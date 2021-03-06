# proxysql-go [![Build Status](https://travis-ci.org/kirinrastogi/proxysql-go.svg?branch=master)](https://travis-ci.org/kirinrastogi/proxysql-go) [![codecov](https://codecov.io/gh/kirinrastogi/proxysql-go/branch/master/graph/badge.svg)](https://codecov.io/gh/kirinrastogi/proxysql-go) [![Documentation](https://godoc.org/github.com/kirinrastogi/proxysql-go?status.svg)](https://godoc.org/github.com/kirinrastogi/proxysql-go/) [![Go Report Card](https://goreportcard.com/badge/github.com/kirinrastogi/proxysql-go)](https://goreportcard.com/report/github.com/kirinrastogi/proxysql-go)


A thread safe package for building ProxySQL sidecars in go. Modify ProxySQL's configuration and routing rules on the fly, and easily write tests for these changes.

# About

Proxysql-go is a package designed to help you build your own service discovery for ProxySQL.
You can update ProxySQL's configuration on the fly. While you could send SQL queries to it yourself, those are hard to mock and test. This package allows you to update it with functions, and then mock and test your code that uses these functions.

# Use

Example located [here](https://github.com/kirinrastogi/proxysql-go/blob/master/example)

### Install the package

```
go get github.com/kirinrastogi/proxysql-go
```

### Import the package

```golang
import (
  . "github.com/kirinrastogi/proxysql-go" // optional "."
)
```

### Create an instance of the client

```golang
conn, err := NewProxySQL("/")
```

The string should be in the [DSN format](https://github.com/go-sql-driver/mysql#dsn-data-source-name)

### Modify ProxySQL's configuration

```golang
err := conn.AddHost(Hostname("some-hostname"), HostgroupID(1))
if err != nil {...}
err = conn.PersistChanges()
if err != nil {...}
// ProxySQL is now using your configuration!
```

# Running Tests

You must have docker installed with privileged access.

To install code dependencies type

```
dep ensure -v
```

Then, run the tests with

```
go test
```

Alternatively, you can run tests that don't require docker with

```
go test -short
```
