package multierr

import "bytes"

// MultiErr is a struct for collecting multiple errors.
// Create a new one by calling New.
type MultiErr struct {
	errs []error
}

// New creates a new, empty MultiErr.
func New() *MultiErr {
	return &MultiErr{}
}

// CollectIfNotNil will collect the given error if it is not
// nil.
//
//	func (c MyComponent) Close() error {
//		errs := multierror.New()
//		errs.CollectIfNotNil(c.field1.Close())
//		errs.CollectIfNotNil(c.field2.Close())
//		errs.CollectIfNotNil(c.field3.Close())
//		errs.CollectIfNotNil(c.field4.Close())
//		return errs.OrNil()
//	}
func (e *MultiErr) CollectIfNotNil(err error) {
	if err != nil {
		e.errs = append(e.errs, err)
	}
}

// OrNil returns an error if more than zero errors were collected on this
// MultiErr, or nil if there weren't any.
func (e MultiErr) OrNil() error {
	if len(e.errs) != 0 {
		return container(e)
	}
	return nil
}

type container MultiErr

func (c container) Error() string {
	if len(c.errs) == 0 {
		return ""
	} else if len(c.errs) == 1 {
		return c.errs[0].Error()
	}

	var buf bytes.Buffer
	buf.WriteString("Multiple errors:")
	for _, err := range c.errs {
		buf.WriteString("\n\t" + err.Error())
	}
	return buf.String()
}
