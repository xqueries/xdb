package command

import "fmt"

type (
	// BinaryExpression describes an expression which has a left and a right hand side argument.
	BinaryExpression interface {
		Expr

		LeftExpr() Expr
		RightExpr() Expr
	}

	// BinaryBase is the base of a binary expression, implementing the Expr interface and holding
	// the left and right hand side argument.
	BinaryBase struct {
		// Left is the left hand side argument of this binary expression.
		Left Expr
		// Right is the right hand side argument of this binary expression.
		Right Expr
	}

	// LessThanExpr represents the binary expression Left < Right.
	LessThanExpr struct {
		BinaryBase
	}

	// GreaterThanExpr represents the binary expression Left > Right.
	GreaterThanExpr struct {
		BinaryBase
	}

	// LessThanOrEqualToExpr represents the binary expression Left <= Right.
	LessThanOrEqualToExpr struct {
		BinaryBase
	}

	// GreaterThanOrEqualToExpr represents the binary expression Left >= Right.
	GreaterThanOrEqualToExpr struct {
		BinaryBase
	}

	// AddExpression represents the binary expression Left + Right.
	AddExpression struct {
		BinaryBase
	}

	// SubExpression represents the binary expression Left - Right.
	SubExpression struct {
		BinaryBase
	}

	// MulExpression represents the binary expression Left * Right.
	MulExpression struct {
		BinaryBase
	}

	// DivExpression represents the binary expression Left / Right.
	DivExpression struct {
		BinaryBase
	}

	// ModExpression represents the binary expression Left % Right.
	ModExpression struct {
		BinaryBase
	}

	// PowExpression represents the binary expression Left ** Right.
	PowExpression struct {
		BinaryBase
	}

	// EqualityExpr represents the binary expression Left == Right.
	// If Invert=true, the expression represents Left != Right.
	EqualityExpr struct {
		BinaryBase
		// Invert determines whether this equality expression must be considered
		// as in-equality expression.
		Invert bool
	}
)

func (BinaryBase) _expr() {}

func (b BinaryBase) toString(op string) string {
	return fmt.Sprintf("%v %v %v", b.Left, op, b.Right)
}

// LeftExpr returns the left hand side argument of the binary expression.
func (b BinaryBase) LeftExpr() Expr {
	return b.Left
}

// RightExpr returns the right hand side argument of the binary expression.
func (b BinaryBase) RightExpr() Expr {
	return b.Right
}

func (e LessThanExpr) String() string {
	return e.toString("<")
}

func (e GreaterThanExpr) String() string {
	return e.toString(">")
}

func (e LessThanOrEqualToExpr) String() string {
	return e.toString("<=")
}

func (e GreaterThanOrEqualToExpr) String() string {
	return e.toString(">=")
}

func (e AddExpression) String() string {
	return e.toString("+")
}

func (e SubExpression) String() string {
	return e.toString("-")
}

func (e MulExpression) String() string {
	return e.toString("*")
}

func (e DivExpression) String() string {
	return e.toString("/")
}

func (e ModExpression) String() string {
	return e.toString("%")
}

func (e PowExpression) String() string {
	return e.toString("**")
}

func (e EqualityExpr) String() string {
	if e.Invert {
		return fmt.Sprintf("%v!=%v", e.Left, e.Right)
	}
	return fmt.Sprintf("%v==%v", e.Left, e.Right)
}
