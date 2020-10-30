package compiler

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsNumeric(t *testing.T) {
	tests := []struct {
		name string
		s    string
		want bool
	}{
		{
			"empty",
			"",
			false,
		},
		{
			"string",
			"hello",
			false,
		},
		{
			"half hex",
			"0x",
			false,
		},
		{
			"hex",
			"0x123ABC",
			true,
		},
		{
			"hex",
			"0xFF",
			true,
		},
		{
			"full hex spectrum",
			"0x0123456789ABCDEF",
			true,
		},
		{
			"full hex spectrum",
			"0xFEDCBA987654321",
			true,
		},
		{
			"small integral",
			"0",
			true,
		},
		{
			"small integral",
			"1",
			true,
		},
		{
			"integral",
			"1234567",
			true,
		},
		{
			"integral",
			"42",
			true,
		},
		{
			"real",
			"0.0",
			true,
		},
		{
			"real",
			".0",
			true,
		},
		{
			"only decimal point",
			".",
			false,
		},
		{
			"real with exponent",
			".0E2",
			true,
		},
		{
			"real with exponent",
			"5.7E-242",
			true,
		},
		{
			"invalid exponent",
			".0e2", // lowercase 'e' is not allowed
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isNumeric(tt.s))
		})
	}
}
