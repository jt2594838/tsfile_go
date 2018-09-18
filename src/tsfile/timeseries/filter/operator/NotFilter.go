package operator

import "tsfile/timeseries/filter"

type NotFilter struct {
	inner filter.Filter
}

func (f *NotFilter) Satisfy(val interface{}) bool {
	return !f.inner.Satisfy(val)
}


