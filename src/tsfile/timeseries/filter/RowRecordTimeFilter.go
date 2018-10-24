package filter

import (
	"tsfile/timeseries/read/datatype"
)

// RowRecordTimeFilter receives a RowRecord and tests if the timestamp of the RowRecord satisfies the inner filter.
type RowRecordTimeFilter struct {
	Filter Filter
}

func NewRowRecordTimeFilter(filter Filter) *RowRecordTimeFilter {
	return &RowRecordTimeFilter{Filter: filter}
}

func (s *RowRecordTimeFilter) Satisfy(val interface{}) bool {
	if m, ok := val.(*datatype.RowRecord); ok {
		return s.Filter.Satisfy(m.Timestamp())
	}
	return false
}
