package types

//go:generate stringer -type=TypeIndicator

// TypeIndicator is a type that is used to serialize types. Each indicator
// corresponds to a type.
type TypeIndicator uint8

// Known type indicators corresponding to known types.
const (
	TypeIndicatorUnknown TypeIndicator = iota
	TypeIndicatorBool
	TypeIndicatorDate
	TypeIndicatorInteger
	TypeIndicatorReal
	TypeIndicatorString
)

var (
	byIndicator = map[TypeIndicator]Type{
		TypeIndicatorBool:    Bool,
		TypeIndicatorDate:    Date,
		TypeIndicatorInteger: Integer,
		TypeIndicatorReal:    Real,
		TypeIndicatorString:  String,
	}
	indicatorFor = map[Type]TypeIndicator{
		Bool:    TypeIndicatorBool,
		Date:    TypeIndicatorDate,
		Integer: TypeIndicatorInteger,
		Real:    TypeIndicatorReal,
		String:  TypeIndicatorString,
	}
)

// ByIndicator accepts a type indicator and returns the corresponding type. If
// the returned type is nil, the type indicator is unknown.
func ByIndicator(indicator TypeIndicator) Type {
	return byIndicator[indicator]
}

// IndicatorFor returns a type indicator for the given Type. If the type is not
// known, TypeIndicatorUnknown will be returned.
func IndicatorFor(t Type) TypeIndicator {
	return indicatorFor[t]
}
