package operator

import "tsfile/timeseries/filter"

type OrFilter struct {
	left filter.Filter
	right filter.Filter
}

func (f *OrFilter) Satisfy(val interface{}) bool {
	return f.left.Satisfy(val) || f.right.Satisfy(val)
}
