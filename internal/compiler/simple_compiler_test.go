package compiler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xqueries/xdb/internal/compiler/command"
	"github.com/xqueries/xdb/internal/parser"
)

type testcase struct {
	name    string
	input   string
	want    command.Command
	wantErr bool
}

func TestSimpleCompiler_Compile_NoOptimizations(t *testing.T) {
	t.Run("select", _TestSimpleCompilerCompileSelectNoOptimizations)
	t.Run("delete", _TestSimpleCompilerCompileDeleteNoOptimizations)
	t.Run("drop", _TestSimpleCompilerCompileDropNoOptimizations)
	t.Run("update", _TestSimpleCompilerCompileUpdateNoOptimizations)
	t.Run("insert", _TestSimpleCompilerCompileInsertNoOptimizations)
	t.Run("negative test", _TestSimpleCompilerNegativeTests)
}

func _TestSimpleCompilerNegativeTests(t *testing.T) {
	tests := []testcase{
		{
			name:    "nothing to select from",
			input:   `WITH myTable AS (SELECT *) SELECT *`,
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, _TestCompile(tt))
	}
}

func _TestSimpleCompilerCompileInsertNoOptimizations(t *testing.T) {
	tests := []testcase{
		{
			"simple insert",
			"INSERT INTO myTable VALUES (1, 2, 3)",
			command.Insert{
				Table: command.SimpleTable{Table: "myTable"},
				Input: command.Values{
					Values: [][]command.Expr{
						{
							command.ConstantLiteral{Value: "1", Numeric: true},
							command.ConstantLiteral{Value: "2", Numeric: true},
							command.ConstantLiteral{Value: "3", Numeric: true},
						},
					},
				},
			},
			false,
		},
		{
			"qualified insert",
			"INSERT INTO mySchema.myTable VALUES (1, 2, 3)",
			command.Insert{
				Table: command.SimpleTable{
					Schema: "mySchema",
					Table:  "myTable",
				},
				Input: command.Values{
					Values: [][]command.Expr{
						{
							command.ConstantLiteral{Value: "1", Numeric: true},
							command.ConstantLiteral{Value: "2", Numeric: true},
							command.ConstantLiteral{Value: "3", Numeric: true},
						},
					},
				},
			},
			false,
		},
		{
			"insert expression list",
			"INSERT INTO mySchema.myTable VALUES (1, 2, 3), (4, 5, 6)",
			command.Insert{
				Table: command.SimpleTable{
					Schema: "mySchema",
					Table:  "myTable",
				},
				Input: command.Values{
					Values: [][]command.Expr{
						{
							command.ConstantLiteral{Value: "1", Numeric: true},
							command.ConstantLiteral{Value: "2", Numeric: true},
							command.ConstantLiteral{Value: "3", Numeric: true},
						},
						{
							command.ConstantLiteral{Value: "4", Numeric: true},
							command.ConstantLiteral{Value: "5", Numeric: true},
							command.ConstantLiteral{Value: "6", Numeric: true},
						},
					},
				},
			},
			false,
		},
		{
			"insert select list",
			"INSERT INTO mySchema.myTable SELECT * FROM myOtherTable",
			command.Insert{
				Table: command.SimpleTable{
					Schema: "mySchema",
					Table:  "myTable",
				},
				Input: command.Project{
					Cols: []command.Column{
						{
							Expr: command.ColumnReference{Name: "*"},
						},
					},
					Input: command.Scan{
						Table: command.SimpleTable{Table: "myOtherTable"},
					},
				},
			},
			false,
		},
		{
			"insert default values",
			"INSERT INTO myTable DEFAULT VALUES",
			command.Insert{
				Table:         command.SimpleTable{Table: "myTable"},
				DefaultValues: true,
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, _TestCompile(tt))
	}
}

func _TestSimpleCompilerCompileUpdateNoOptimizations(t *testing.T) {
	tests := []testcase{
		{
			"simple update",
			"UPDATE myTable SET myCol = 7",
			command.Update{
				UpdateOr: command.UpdateOrIgnore, // default
				Table: command.SimpleTable{
					Table: "myTable",
				},
				Filter: command.ConstantBooleanExpr{Value: true},
				Updates: []command.UpdateSetter{
					{
						Cols:  []string{"myCol"},
						Value: command.ConstantLiteral{Value: "7", Numeric: true},
					},
				},
			},
			false,
		},
		{
			"filtered update",
			"UPDATE myTable SET myCol = 7 WHERE myOtherCol == 9",
			command.Update{
				UpdateOr: command.UpdateOrIgnore, // default
				Table: command.SimpleTable{
					Table: "myTable",
				},
				Filter: command.EqualityExpr{
					BinaryBase: command.BinaryBase{
						Left:  command.ColumnReference{Name: "myOtherCol"},
						Right: command.ConstantLiteral{Value: "9", Numeric: true},
					},
				},
				Updates: []command.UpdateSetter{
					{
						Cols:  []string{"myCol"},
						Value: command.ConstantLiteral{Value: "7", Numeric: true},
					},
				},
			},
			false,
		},
		{
			"filtered update or fail",
			"UPDATE OR FAIL myTable SET myCol = 7 WHERE myOtherCol == 9",
			command.Update{
				UpdateOr: command.UpdateOrFail,
				Table: command.SimpleTable{
					Table: "myTable",
				},
				Filter: command.EqualityExpr{
					BinaryBase: command.BinaryBase{
						Left:  command.ColumnReference{Name: "myOtherCol"},
						Right: command.ConstantLiteral{Value: "9", Numeric: true},
					},
				},
				Updates: []command.UpdateSetter{
					{
						Cols:  []string{"myCol"},
						Value: command.ConstantLiteral{Value: "7", Numeric: true},
					},
				},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, _TestCompile(tt))
	}
}

func _TestSimpleCompilerCompileDropNoOptimizations(t *testing.T) {
	tests := []testcase{
		// table
		{
			"simple drop table",
			"DROP TABLE myTable",
			command.DropTable{
				Name: "myTable",
			},
			false,
		},
		{
			"simple drop table if exists",
			"DROP TABLE IF EXISTS myTable",
			command.DropTable{
				Name:     "myTable",
				IfExists: true,
			},
			false,
		},
		{
			"qualified drop table",
			"DROP TABLE mySchema.myTable",
			command.DropTable{
				Schema: "mySchema",
				Name:   "myTable",
			},
			false,
		},
		{
			"qualified drop table if exists",
			"DROP TABLE IF EXISTS mySchema.myTable",
			command.DropTable{
				Schema:   "mySchema",
				Name:     "myTable",
				IfExists: true,
			},
			false,
		},
		// view
		{
			"simple drop view",
			"DROP VIEW myView",
			command.DropView{
				Name: "myView",
			},
			false,
		},
		{
			"simple drop view if exists",
			"DROP VIEW IF EXISTS myView",
			command.DropView{
				Name:     "myView",
				IfExists: true,
			},
			false,
		},
		{
			"qualified drop view",
			"DROP VIEW mySchema.myView",
			command.DropView{
				Schema: "mySchema",
				Name:   "myView",
			},
			false,
		},
		{
			"qualified drop view if exists",
			"DROP VIEW IF EXISTS mySchema.myView",
			command.DropView{
				Schema:   "mySchema",
				Name:     "myView",
				IfExists: true,
			},
			false,
		},
		// index
		{
			"simple drop index",
			"DROP INDEX myIndex",
			command.DropIndex{
				Name: "myIndex",
			},
			false,
		},
		{
			"simple drop index if exists",
			"DROP INDEX IF EXISTS myIndex",
			command.DropIndex{
				Name:     "myIndex",
				IfExists: true,
			},
			false,
		},
		{
			"qualified drop index",
			"DROP INDEX mySchema.myIndex",
			command.DropIndex{
				Schema: "mySchema",
				Name:   "myIndex",
			},
			false,
		},
		{
			"qualified drop index if exists",
			"DROP INDEX IF EXISTS mySchema.myIndex",
			command.DropIndex{
				Schema:   "mySchema",
				Name:     "myIndex",
				IfExists: true,
			},
			false,
		},
		// trigger
		{
			"simple drop trigger",
			"DROP TRIGGER myTrigger",
			command.DropTrigger{
				Name: "myTrigger",
			},
			false,
		},
		{
			"simple drop trigger if exists",
			"DROP TRIGGER IF EXISTS myTrigger",
			command.DropTrigger{
				Name:     "myTrigger",
				IfExists: true,
			},
			false,
		},
		{
			"qualified drop trigger",
			"DROP TRIGGER mySchema.myTrigger",
			command.DropTrigger{
				Schema: "mySchema",
				Name:   "myTrigger",
			},
			false,
		},
		{
			"qualified drop trigger if exists",
			"DROP TRIGGER IF EXISTS mySchema.myTrigger",
			command.DropTrigger{
				Schema:   "mySchema",
				Name:     "myTrigger",
				IfExists: true,
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, _TestCompile(tt))
	}
}

func _TestSimpleCompilerCompileDeleteNoOptimizations(t *testing.T) {
	tests := []testcase{
		{
			"simple delete",
			"DELETE FROM myTable",
			command.Delete{
				Table: command.SimpleTable{
					Table: "myTable",
				},
				Filter: command.ConstantBooleanExpr{Value: true},
			},
			false,
		},
		{
			"qualified delete",
			"DELETE FROM mySchema.myTable",
			command.Delete{
				Table: command.SimpleTable{
					Table:  "myTable",
					Schema: "mySchema",
				},
				Filter: command.ConstantBooleanExpr{Value: true},
			},
			false,
		},
		{
			"delete with filter",
			"DELETE FROM myTable WHERE col1 == col2",
			command.Delete{
				Table: command.SimpleTable{
					Table: "myTable",
				},
				Filter: command.EqualityExpr{
					BinaryBase: command.BinaryBase{
						Left:  command.ColumnReference{Name: "col1"},
						Right: command.ColumnReference{Name: "col2"},
					},
				},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, _TestCompile(tt))
	}
}

func _TestSimpleCompilerCompileSelectNoOptimizations(t *testing.T) {
	tests := []testcase{
		{
			"simple values",
			"VALUES (1,2,3),(4,5,6),(7,8,9)",
			command.Values{
				Values: [][]command.Expr{
					{
						command.ConstantLiteral{Value: "1", Numeric: true},
						command.ConstantLiteral{Value: "2", Numeric: true},
						command.ConstantLiteral{Value: "3", Numeric: true},
					},
					{
						command.ConstantLiteral{Value: "4", Numeric: true},
						command.ConstantLiteral{Value: "5", Numeric: true},
						command.ConstantLiteral{Value: "6", Numeric: true},
					},
					{
						command.ConstantLiteral{Value: "7", Numeric: true},
						command.ConstantLiteral{Value: "8", Numeric: true},
						command.ConstantLiteral{Value: "9", Numeric: true},
					},
				},
			},
			false,
		},
		{
			"simple select",
			"SELECT * FROM myTable",
			command.Project{
				Cols: []command.Column{
					{
						Expr: command.ColumnReference{Name: "*"},
					},
				},
				Input: command.Scan{
					Table: command.SimpleTable{
						Table: "myTable",
					},
				},
			},
			false,
		},
		{
			"simple select where",
			"SELECT * FROM myTable WHERE true",
			command.Project{
				Cols: []command.Column{
					{
						Expr: command.ColumnReference{Name: "*"},
					},
				},
				Input: command.Select{
					Filter: command.ConstantBooleanExpr{Value: true},
					Input: command.Scan{
						Table: command.SimpleTable{
							Table: "myTable",
						},
					},
				},
			},
			false,
		},
		{
			"simple select limit",
			"SELECT * FROM myTable LIMIT 5",
			command.Limit{
				Limit: command.ConstantLiteral{Value: "5", Numeric: true},
				Input: command.Project{
					Cols: []command.Column{
						{
							Expr: command.ColumnReference{Name: "*"},
						},
					},
					Input: command.Scan{
						Table: command.SimpleTable{
							Table: "myTable",
						},
					},
				},
			},
			false,
		},
		{
			"simple select limit offset",
			"SELECT * FROM myTable LIMIT 5, 10",
			command.Limit{
				Limit: command.ConstantLiteral{Value: "5", Numeric: true},
				Input: command.Offset{
					Offset: command.ConstantLiteral{Value: "10", Numeric: true},
					Input: command.Project{
						Cols: []command.Column{
							{
								Expr: command.ColumnReference{Name: "*"},
							},
						},
						Input: command.Scan{
							Table: command.SimpleTable{
								Table: "myTable",
							},
						},
					},
				},
			},
			false,
		},
		{
			"simple select limit offset",
			"SELECT * FROM myTable LIMIT 5 OFFSET 10",
			command.Limit{
				Limit: command.ConstantLiteral{Value: "5", Numeric: true},
				Input: command.Offset{
					Offset: command.ConstantLiteral{Value: "10", Numeric: true},
					Input: command.Project{
						Cols: []command.Column{
							{
								Expr: command.ColumnReference{Name: "*"},
							},
						},
						Input: command.Scan{
							Table: command.SimpleTable{
								Table: "myTable",
							},
						},
					},
				},
			},
			false,
		},
		{
			"select distinct",
			"SELECT DISTINCT * FROM myTable WHERE true",
			command.Distinct{
				Input: command.Project{
					Cols: []command.Column{
						{
							Expr: command.ColumnReference{Name: "*"},
						},
					},
					Input: command.Select{
						Filter: command.ConstantBooleanExpr{Value: true},
						Input: command.Scan{
							Table: command.SimpleTable{
								Table: "myTable",
							},
						},
					},
				},
			},
			false,
		},
		{
			"select with implicit join",
			"SELECT * FROM a, b WHERE true",
			command.Project{
				Cols: []command.Column{
					{
						Expr: command.ColumnReference{Name: "*"},
					},
				},
				Input: command.Select{
					Filter: command.ConstantBooleanExpr{Value: true},
					Input: command.Join{
						Left: command.Scan{
							Table: command.SimpleTable{
								Table: "a",
							},
						},
						Right: command.Scan{
							Table: command.SimpleTable{
								Table: "b",
							},
						},
					},
				},
			},
			false,
		},
		{
			"select with explicit join",
			"SELECT * FROM a JOIN b WHERE true",
			command.Project{
				Cols: []command.Column{
					{
						Expr: command.ColumnReference{Name: "*"},
					},
				},
				Input: command.Select{
					Filter: command.ConstantBooleanExpr{Value: true},
					Input: command.Join{
						Left: command.Scan{
							Table: command.SimpleTable{
								Table: "a",
							},
						},
						Right: command.Scan{
							Table: command.SimpleTable{
								Table: "b",
							},
						},
					},
				},
			},
			false,
		},
		{
			"select with implicit and explicit join",
			"SELECT * FROM a, b JOIN c WHERE true",
			command.Project{
				Cols: []command.Column{
					{
						Expr: command.ColumnReference{Name: "*"},
					},
				},
				Input: command.Select{
					Filter: command.ConstantBooleanExpr{Value: true},
					Input: command.Join{
						Left: command.Join{
							Left: command.Scan{
								Table: command.SimpleTable{
									Table: "a",
								},
							},
							Right: command.Scan{
								Table: command.SimpleTable{
									Table: "b",
								},
							},
						},
						Right: command.Scan{
							Table: command.SimpleTable{
								Table: "c",
							},
						},
					},
				},
			},
			false,
		},
		{
			"select expression",
			"SELECT name, amount * price AS total_price FROM items JOIN prices",
			command.Project{
				Cols: []command.Column{
					{
						Expr: command.ColumnReference{Name: "name"},
					},
					{
						Expr: command.MulExpression{
							BinaryBase: command.BinaryBase{
								Left:  command.ColumnReference{Name: "amount"},
								Right: command.ColumnReference{Name: "price"},
							},
						},
						Alias: "total_price",
					},
				},
				Input: command.Join{
					Left: command.Scan{
						Table: command.SimpleTable{Table: "items"},
					},
					Right: command.Scan{
						Table: command.SimpleTable{Table: "prices"},
					},
				},
			},
			false,
		},
		{
			"select function",
			"SELECT AVG(price) AS avg_price FROM items LEFT JOIN prices",
			command.Project{
				Cols: []command.Column{
					{
						Expr: command.FunctionExpr{
							Name:     "AVG",
							Distinct: false,
							Args: []command.Expr{
								command.ColumnReference{Name: "price"},
							},
						},
						Alias: "avg_price",
					},
				},
				Input: command.Join{
					Type: command.JoinLeft,
					Left: command.Scan{
						Table: command.SimpleTable{Table: "items"},
					},
					Right: command.Scan{
						Table: command.SimpleTable{Table: "prices"},
					},
				},
			},
			false,
		},
		{
			"select function distinct",
			"SELECT AVG(DISTINCT price) AS avg_price FROM items LEFT JOIN prices",
			command.Project{
				Cols: []command.Column{
					{
						Expr: command.FunctionExpr{
							Name:     "AVG",
							Distinct: true,
							Args: []command.Expr{
								command.ColumnReference{Name: "price"},
							},
						},
						Alias: "avg_price",
					},
				},
				Input: command.Join{
					Type: command.JoinLeft,
					Left: command.Scan{
						Table: command.SimpleTable{Table: "items"},
					},
					Right: command.Scan{
						Table: command.SimpleTable{Table: "prices"},
					},
				},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, _TestCompile(tt))
	}
}

func _TestCompile(tt testcase) func(t *testing.T) {
	return func(t *testing.T) {
		assert := assert.New(t)

		c := &simpleCompiler{}
		p, err := parser.New(tt.input)
		assert.NoError(err)

		stmt, errs, ok := p.Next()
		assert.Len(errs, 0)
		assert.True(ok)

		got, gotErr := c.Compile(stmt)

		if tt.wantErr {
			assert.Error(gotErr)
		} else {
			assert.NoError(gotErr)
		}
		assert.Equal(tt.want, got)
	}
}
