package main

import (
	"github.com/kirinrastogi/proxysql-go/mocks"
	"testing"
)

func TestInsertWriterInsertsWriter(t *testing.T) {
	mock := proxysql_mock.New()
	InsertWriter(mock)
	if w, _ := mock.Writer(); w != "some-host" {
		t.Fail()
	}
}
