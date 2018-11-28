# proxysql-go [![Build Status](https://travis-ci.org/kirinrastogi/proxysql-go.svg?branch=master)](https://travis-ci.org/kirinrastogi/proxysql-go) [![codecov](https://codecov.io/gh/kirinrastogi/proxysql-go/branch/master/graph/badge.svg)](https://codecov.io/gh/kirinrastogi/proxysql-go)


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
