package operator

import "tsfile/timeseries/filter"

type AndFilter struct {
	left filter.Filter
	right filter.Filter
}

func (f *AndFilter) Satisfy(val interface{}) bool {
	return f.left.Satisfy(val) && f.right.Satisfy(val)
}
