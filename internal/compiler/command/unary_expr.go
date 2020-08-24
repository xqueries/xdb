package command

import "fmt"

type (
	// UnaryBase is the base of all unary expressions, holding a single value.
	UnaryBase struct {
		Value Expr
	}

	// UnaryNegativeExpr represents the unary expression -X, where X is the value
	// of the expression.
	UnaryNegativeExpr struct {
		UnaryBase
	}

	// UnaryBitwiseNegationExpr represents the unary expression ~X, where X is the value
	// of the expression.
	UnaryBitwiseNegationExpr struct {
		UnaryBase
	}

	// UnaryNegationExpr represents the unary expression NOT X, where X is the value
	// of the expression.
	UnaryNegationExpr struct {
		UnaryBase
	}
)

func (UnaryBase) _expr() {}

func (b UnaryBase) toString(op string) string {
	return fmt.Sprintf("%v %v", op, b.Value)
}

func (e UnaryNegativeExpr) String() string {
	return e.toString("-")
}

func (e UnaryBitwiseNegationExpr) String() string {
	return e.toString("~")
}

func (e UnaryNegationExpr) String() string {
	return e.toString("NOT")
}
