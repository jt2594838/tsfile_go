package filter

import (
	"tsfile/timeseries/read/datatype"
	"tsfile/common/constant"
)

// RowRecordTimeFilter receives a RowRecord and tests whether a certain column in the RowRecord specified by its name
// satisfies the inner filter.
// NOTICE：The schema of the input (number of columns and each's name) should remain the same for the same filter.
// E.g: If you use the filter (seriesName is "s2") on a RowRecord with three cols [s0, s1, s2], then you cannot use this
// filter on a RowRecord with cols [s1, s2, s3]. Because the filter remembers that "s2" is the third col and will not re-locate
// it in future tests.
type RowRecordValFilter struct {
	seriesName string
	filter Filter

	seriesIndex int
}

func NewRowRecordValFilter(seriesName string, filter Filter) *RowRecordValFilter {
	return &RowRecordValFilter{seriesName:seriesName, filter:filter, seriesIndex:constant.INDEX_NOT_SET}
}

func (s *RowRecordValFilter) Satisfy(val interface{}) bool {
	if m, ok := val.(*datatype.RowRecord); ok {
		if s.seriesIndex == constant.INDEX_NOT_SET {
			for i, path := range m.Paths() {
				if path == s.seriesName {
					s.seriesIndex = i
					return s.filter.Satisfy(m.Values()[i])
				}
			}
		}

		return s.filter.Satisfy(m.Values()[s.seriesIndex])
	}
	return false
}

