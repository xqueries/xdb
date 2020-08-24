package types

// Caster wraps the Cast method, which is used to transform the input value
// into an output value. Types can implement this interface. E.g. if the
// type String implements Caster, any value passed into the Cast method
// should be attempted to be cast to String, or an error should be returned.
type Caster interface {
	Cast(Value) (Value, error)
}

// Cast attempts to cast the given value to the given target type. If the caster
// cannot cast the given value, false will be returned, otherwise, the casted
// value will be returned. To obtain the casting error, call (Caster).Cast instead.
func Cast(val Value, target Caster) (Value, bool) {
	castedVal, err := target.Cast(val)
	return castedVal, err == nil
}
