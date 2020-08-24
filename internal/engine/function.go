package engine

import (
	"github.com/xqueries/xdb/internal/engine/types"
)

func (e Engine) evaluateFunction(ctx ExecutionContext, fn types.FunctionValue) (types.Value, error) {
	switch fn.Name {
	case "NOW":
		return e.builtinNow(e.timeProvider)
	case "RANDOM":
		return e.builtinRand(e.randomProvider)
	}
	return nil, ErrNoSuchFunction(fn.Name)
}
