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
				Expr:  command.ColumnReference{Name: "col1"},
				Alias: "myCol",
			},
			{
				Expr: command.ColumnReference{Name: "col2"},
			},
		},
		Input: command.Select{
			Filter: command.EqualityExpr{
				BinaryBase: command.BinaryBase{
					Left: command.ColumnReference{
						Name: "foobar",
					},
					Right: command.ColumnReference{
						Name: "snafu",
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
