// This file contains implementations for builtin functions, such as RAND() or
// NOW(). The arguments for the herein implemented functions differ from those
// that are required in the SQL statement. For example, COUNT(x) takes only one
// argument, but builtinCount requires many values. The engine is responsible to
// interpret COUNT(x), and instead of the single value 'x', pass in all values
// in the column 'x'. How SQL arguments are to be interpreted, depends on the
// SQL function. The builtin functions in this file don't access the result
// table, but instead rely on the engine to pass in the correct values.

package engine

import (
	"fmt"
	"strings"

	"github.com/xqueries/xdb/internal/engine/types"
)

// builtinNow returns a new date value, containing the timestamp provided by the
// given timeProvider.
func (e Engine) builtinNow(tp timeProvider) (types.DateValue, error) {
	defer e.profiler.Enter("now").Exit()

	return types.NewDate(tp()), nil
}

func (e Engine) builtinRand(rp randomProvider) (types.IntegerValue, error) {
	defer e.profiler.Enter("rand").Exit()

	return types.NewInteger(rp()), nil
}

// builtinCount returns a new integral value, representing the count of the
// passed in values.
func (e Engine) builtinCount(args ...types.Value) (types.IntegerValue, error) {
	defer e.profiler.Enter("count").Exit()

	return types.NewInteger(int64(len(args))), nil
}

// builtinUCase maps all passed in string values to new string values with the
// internal string value folded to upper case.
func (e Engine) builtinUCase(args ...types.StringValue) ([]types.StringValue, error) {
	defer e.profiler.Enter("ucase").Exit()

	var output []types.StringValue
	for _, arg := range args {
		output = append(output, types.StringValue{Value: strings.ToUpper(arg.Value)})
	}
	return output, nil
}

// builtinLCase maps all passed in string values to new string values with the
// internal string value folded to lower case.
func (e Engine) builtinLCase(args ...types.StringValue) ([]types.StringValue, error) {
	defer e.profiler.Enter("lcase").Exit()

	var output []types.StringValue
	for _, arg := range args {
		output = append(output, types.StringValue{Value: strings.ToLower(arg.Value)})
	}
	return output, nil
}

// builtinMax returns the largest value out of all passed in values. The largest
// value is determined by comparing one element to all others.
func (e Engine) builtinMax(args ...types.Value) (types.Value, error) {
	defer e.profiler.Enter("max").Exit()

	if len(args) == 0 {
		return nil, nil
	}

	if err := ensureSameType(args...); err != nil {
		return nil, err
	}

	largest := args[0] // start at 0 and compare on
	t := largest.Type()
	comparator, ok := t.(types.Comparator)
	if !ok {
		return nil, ErrUncomparable(t)
	}
	for i := 1; i < len(args); i++ {
		res, err := comparator.Compare(largest, args[i])
		if err != nil {
			return nil, fmt.Errorf("compare: %w", err)
		}
		if res < 0 {
			largest = args[i]
		}
	}
	return largest, nil
}

// builtinMin returns the smallest value out of all passed in values. The
// smallest value is determined by comparing one element to all others.
func (e Engine) builtinMin(args ...types.Value) (types.Value, error) {
	defer e.profiler.Enter("min").Exit()

	if len(args) == 0 {
		return nil, nil
	}

	if err := ensureSameType(args...); err != nil {
		return nil, err
	}

	smallest := args[0]
	t := smallest.Type()
	comparator, ok := t.(types.Comparator)
	if !ok {
		return nil, ErrUncomparable(t)
	}
	for i := 1; i < len(args); i++ {
		res, err := comparator.Compare(smallest, args[i])
		if err != nil {
			return nil, fmt.Errorf("compare: %w", err)
		}
		if res > 0 {
			smallest = args[i]
		}
	}
	return smallest, nil
}

// ensureSameType returns an error if not all given values have the same type.
func ensureSameType(args ...types.Value) error {
	if len(args) == 0 {
		return nil
	}

	base := args[0]
	for i := 1; i < len(args); i++ {
		if !base.Is(args[i].Type()) { // Is is transitive
			return types.ErrTypeMismatch(base.Type(), args[i].Type())
		}
	}
	return nil
}
