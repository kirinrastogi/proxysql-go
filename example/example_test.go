package main

import (
	"testing"
)

// Test functionality of AddHostsAndSave
func TestAddHostsAndSaveErrorsOnConnectionFailure(t *testing.T) {
	mock := NewProxySQLMock()
	mock.healthy = false
	err := AddHostsAndSave(mock)
	if err == nil {
		t.Fatal("AddHostsAndSave did not return error on ping fail")
	}
	t.Log(err)
}
