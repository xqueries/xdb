package test

import (
	"testing"
)

func TestIssue35(t *testing.T) {
	RunAndCompare(t, Test{
		Name:      "issue35",
		Statement: `SELECT "abc" AS myCol`,
	})
}
