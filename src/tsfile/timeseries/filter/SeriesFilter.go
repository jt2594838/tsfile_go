package filter

import "tsfile/timeseries/read/datatype"

type SeriesFilter struct {
	seriesName string
	filter Filter
}

func (s *SeriesFilter) Satisfy(val interface{}) bool {
	if m, ok := val.(datatype.RowRecord); ok {
		for i, path := range m.Paths() {
			if path == s.seriesName {
				return s.filter.Satisfy(m.Values()[i])
			}
		}
	}
	return false
}

