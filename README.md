# proxysql-go [![Build Status](https://travis-ci.org/kirinrastogi/proxysql-go.svg?branch=master)](https://travis-ci.org/kirinrastogi/proxysql-go) [![codecov](https://codecov.io/gh/kirinrastogi/proxysql-go/branch/master/graph/badge.svg)](https://codecov.io/gh/kirinrastogi/proxysql-go) [![Documentation](https://godoc.org/github.com/kirinrastogi/proxysql-go?status.svg)](https://godoc.org/github.com/kirinrastogi/proxysql-go/)


A ProxySQL client in go

# Use

Example located [here](https://github.com/kirinrastogi/proxysql-go/blob/master/example/example.go)

### Import the package

```golang
import (
  . "github.com/kirinrastogi/proxysql-go"
)
```

### Create an instance of the client

```golang
conn, err := NewProxySQL("/")
```

### Modify ProxySQL's configuration

```golang
err := conn.AddHost(Hostname("some-hostname"), Hostgroup(1))
if err != nil {...}
err = conn.PersistChanges()
```

# Running Tests

You must have docker installed with privileged access.

Run the tests with

```bash
go test
```

You can run tests that don't require docker with

```bash
go test -short
```
