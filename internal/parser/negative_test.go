package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type NegativeTest struct {
	Name  string
	Query string
}

func (nt NegativeTest) Run(t *testing.T) {
	t.Helper()
	t.Run(nt.Name, func(t *testing.T) {
		assert := assert.New(t)

		p, err := New(nt.Query)
		assert.NoError(err)
		_, errs, ok := p.Next()
		assert.True(ok, "expected exactly one statement")
		assert.NotEmpty(errs, "expected errors")

		_, _, ok = p.Next()
		assert.False(ok, "expected only one statement")
	})
}
