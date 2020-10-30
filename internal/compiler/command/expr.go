package command

import (
	"fmt"
	"strconv"
	"strings"
)

type (
	// Expr is a marker interface for anything that is an expression. Different
	// implementations of this interface represent different productions of the
	// expression rule in the SQL grammar.
	Expr interface {
		fmt.Stringer
		_expr()
	}

	// ConstantLiteral is a constant literal expression. The Numeric flag determines whether
	// or not this is a numeric literal.
	ConstantLiteral struct {
		Value   string
		Numeric bool
	}

	// ConstantLiteralOrColumnReference is a string literal that represents either a constant
	// string value or a column reference. If this literal is resolved, the column reference
	// takes precedence over the string literal, meaning that if a column exists, whose name is
	// equal to the value of this expression, the value in that column must be used. Only if there
	// is no such column present, the string literal is to be used.
	ConstantLiteralOrColumnReference struct {
		ValueOrName string
	}

	// ColumnReference is a string literal that represents a reference to a column. During resolving,
	// if no column with such a name is present, an error must be risen.
	ColumnReference struct {
		Name string
	}

	// ConstantBooleanExpr is a simple expression that represents a boolean
	// value. It is rarely emitted by the compiler and rather used by
	// optimizations.
	ConstantBooleanExpr struct {
		// Value is the simple bool value of this expression.
		Value bool
	}

	// FunctionExpr represents a function call expression.
	FunctionExpr struct {
		// Name is the name of the function.
		Name string
		// Distinct determines, whether only distinct elements in the arguments'
		// input lists must be considered.
		Distinct bool
		// Args are the function argument expressions.
		Args []Expr
	}

	// RangeExpr is an expression with a needle, an upper and a lower bound. It
	// must be evaluated to true, if needle is within the lower and upper bound,
	// or if the needle is not between the bounds and the range is inverted.
	RangeExpr struct {
		// Needle is the value that is evaluated if it is between Lo and Hi.
		Needle Expr
		// Lo is the lower bound of this range.
		Lo Expr
		// Hi is the upper bound of this range.
		Hi Expr
		// Invert determines if Needle must be between or not between the bounds
		// of this range.
		Invert bool
	}
)

func (ConstantBooleanExpr) _expr() {}
func (RangeExpr) _expr()           {}
func (FunctionExpr) _expr()        {}

func (ConstantLiteral) _expr()                  {}
func (ConstantLiteralOrColumnReference) _expr() {}
func (ColumnReference) _expr()                  {}

// ConstantValue returns the constant value of this literal.
func (l ConstantLiteral) ConstantValue() string {
	return l.Value
}

func (l ConstantLiteral) String() string {
	return l.Value
}

// ConstantValue returns the constant value of this literal.
func (l ConstantLiteralOrColumnReference) ConstantValue() string {
	return l.ValueOrName
}

// ReferencedColName returns the constant name of the referenced column name.
func (l ConstantLiteralOrColumnReference) ReferencedColName() string {
	return l.ValueOrName
}

func (l ConstantLiteralOrColumnReference) String() string {
	return l.ValueOrName
}

// ReferencedColName returns the constant name of the referenced column name.
func (l ColumnReference) ReferencedColName() string {
	return l.Name
}

func (l ColumnReference) String() string {
	return l.Name
}

func (b ConstantBooleanExpr) String() string {
	return strconv.FormatBool(b.Value)
}

func (r RangeExpr) String() string {
	if r.Invert {
		return fmt.Sprintf("![%v;%v]", r.Lo, r.Hi)
	}
	return fmt.Sprintf("[%v;%v]", r.Lo, r.Hi)
}

func (f FunctionExpr) String() string {
	var args []string
	for _, arg := range f.Args {
		args = append(args, arg.String())
	}
	if f.Distinct {
		return fmt.Sprintf("%s(DISTINCT %s)", f.Name, strings.Join(args, ","))
	}
	return fmt.Sprintf("%s(%s)", f.Name, strings.Join(args, ","))
}
