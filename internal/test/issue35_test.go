package test

import (
	"testing"
)

func TestIssue35(t *testing.T) {
	RunAndCompare(t, Testcase{
		Name:      "issue35",
		Statement: `SELECT "abc" AS myCol`,
	})
}
