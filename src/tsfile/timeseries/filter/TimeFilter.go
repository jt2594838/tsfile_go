package filter

import "tsfile/timeseries/read/datatype"

type TimeFilter struct {
	filter Filter
}

func (s *TimeFilter) Satisfy(val interface{}) bool {
	if m, ok := val.(datatype.RowRecord); ok {
		s.filter.Satisfy(m.Timestamp())
	}
	return false
}
