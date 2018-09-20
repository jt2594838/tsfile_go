package filter

import "tsfile/timeseries/read/datatype"

type RowRecordValFilter struct {
	seriesName string
	filter Filter
}

func (s *RowRecordValFilter) Satisfy(val interface{}) bool {
	if m, ok := val.(datatype.RowRecord); ok {
		for i, path := range m.Paths() {
			if path == s.seriesName {
				return s.filter.Satisfy(m.Values()[i])
			}
		}
	}
	return false
}

