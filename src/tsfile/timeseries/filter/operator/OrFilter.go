package operator

import "tsfile/timeseries/filter"

type OrFilter struct {
	filters []filter.Filter
}

func (f *OrFilter) Satisfy(val interface{}) bool {
	if f.filters == nil {
		return true
	}

	for _, filt := range f.filters {
		if filt.Satisfy(val) {
			return true
		}
	}
	return false
}
