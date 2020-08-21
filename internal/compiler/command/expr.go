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

	// LiteralExpr is a simple literal expression that has a single string
	// value.
	LiteralExpr struct {
		// Value is the simple string value of this expression.
		Value string
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

func (LiteralExpr) _expr()         {}
func (ConstantBooleanExpr) _expr() {}
func (RangeExpr) _expr()           {}
func (FunctionExpr) _expr()        {}

func (l LiteralExpr) String() string {
	return l.Value
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
