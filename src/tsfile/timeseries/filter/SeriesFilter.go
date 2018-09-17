package filter

import "tsfile/timeseries/read/datatype"

type SeriesFilter struct {
	seriesName string
	filter Filter
}

func (s *SeriesFilter) satisfy(val interface{}) bool {
	if m, ok := val.(datatype.RowRecord); ok {
		if v, ok := m[s.seriesName]; ok {
			return s.filter.satisfy(v)
		} else {
			return false
		}
	}
	return false
}

