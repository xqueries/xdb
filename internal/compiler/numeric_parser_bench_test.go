package compiler

import (
	"testing"
)

func BenchmarkToNumericValue(b *testing.B) {
	str := "75610342.92389E-21423"

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if !isNumeric(str) {
			b.FailNow()
		}
	}
}
