package operator

import "tsfile/timeseries/filter"

// NotFilter returns true iff the value DOES NOT satisfy the inner filter.
// NOTICE: !(3.0 < "dd") will return true because (3.0 < "dd") returns false due to type mismatch.
type NotFilter struct {
	inner filter.Filter
}

func (f *NotFilter) Satisfy(val interface{}) bool {
	return !f.inner.Satisfy(val)
}
