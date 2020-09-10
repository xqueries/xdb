package optimization

import (
	"testing"

	"github.com/xqueries/xdb/internal/compiler/command"
)

var result interface{}

func Benchmark_OptHalfJoin(b *testing.B) {
	cmd := command.Project{
		Cols: []command.Column{
			{
				Name:  command.LiteralExpr{Value: "col1"},
				Alias: "myCol",
			},
			{Name: command.LiteralExpr{Value: "col2"}},
		},
		Input: command.Select{
			Filter: command.EqualityExpr{
				BinaryBase: command.BinaryBase{
					Left: command.LiteralExpr{
						Value: "foobar",
					},
					Right: command.LiteralExpr{
						Value: "snafu",
					},
				},
				Invert: true,
			},
			Input: command.Join{
				Left: nil,
				Right: command.Scan{
					Table: command.SimpleTable{Table: "foobar"},
				},
			},
		},
	}

	var r interface{}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		r, _ = OptHalfJoin(cmd)
	}

	result = r
}
