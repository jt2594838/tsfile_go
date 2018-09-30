package operator

import "tsfile/timeseries/filter"

// AndFilter returns true iff the value satisfies all its children or it has no children.
type AndFilter struct {
	Filters []filter.Filter
}

func (f *AndFilter) Satisfy(val interface{}) bool {
	if f.Filters == nil {
		return true
	}

	for _, filt := range f.Filters {
		if !filt.Satisfy(val) {
			return false
		}
	}
	return true
}
