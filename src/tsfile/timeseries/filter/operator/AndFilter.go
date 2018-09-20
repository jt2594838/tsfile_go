package operator

import "tsfile/timeseries/filter"

type AndFilter struct {
	filters []filter.Filter
}

func (f *AndFilter) Satisfy(val interface{}) bool {
	if f.filters == nil {
		return true
	}

	for _, filt := range f.filters {
		if !filt.Satisfy(val) {
			return false
		}
	}
	return true
}
