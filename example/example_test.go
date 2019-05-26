package main

import (
	"testing"
)

// Test functionality of addHostsAndSave
func TestAddHostsAndSaveErrorsOnConnectionFailure(t *testing.T) {
	mock := NewProxySQLMock()
	mock.healthy = false
	err := addHostsAndSave(mock)
	if err == nil {
		t.Fatal("AddHostsAndSave did not return error on ping fail")
	}
	t.Log(err)
}
