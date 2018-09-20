package filter

import "tsfile/timeseries/read/datatype"

type RowRecordTimeFilter struct {
	filter Filter
}

func (s *RowRecordTimeFilter) Satisfy(val interface{}) bool {
	if m, ok := val.(datatype.RowRecord); ok {
		s.filter.Satisfy(m.Timestamp())
	}
	return false
}
