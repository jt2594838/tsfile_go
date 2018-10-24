package impl

import (
	"tsfile/common/utils"
	"tsfile/timeseries/filter"
	"tsfile/timeseries/read/datatype"
	"tsfile/timeseries/read/reader"
	"tsfile/timeseries/read/reader/impl/basic"
)

// MergeDataSet merges paths in the select clause and where clause together and constructs rows using all the paths
// and applies the filter on each row.
type MergeQueryDataSet struct {
	reader reader.IRowRecordReader

	row         *datatype.RowRecord
	selectPaths []string
	pathIndex   map[string]int
}

func NewMergeQueryDataSet(selectPaths []string, conditionPaths []string, readerMap map[string]reader.TimeValuePairReader,
	filter filter.Filter) *MergeQueryDataSet {
	allPaths := utils.MergeStrings(selectPaths, conditionPaths)
	rowReader := basic.NewFilteredRowReader(allPaths, readerMap, filter)
	dataSet := &MergeQueryDataSet{reader: rowReader}
	dataSet.row = datatype.NewRowRecordWithPaths(selectPaths)
	dataSet.selectPaths = selectPaths
	dataSet.pathIndex = make(map[string]int, len(selectPaths))
	for i, aPath := range allPaths {
		for _, sPath := range selectPaths {
			if aPath == sPath {
				dataSet.pathIndex[sPath] = i
			}
		}
	}

	return dataSet
}

func (set *MergeQueryDataSet) HasNext() bool {
	return set.reader.HasNext()
}

func (set *MergeQueryDataSet) Next() (*datatype.RowRecord, error) {
	row, err := set.reader.Next()
	if err != nil {
		return nil, err
	}
	for i, path := range set.selectPaths {
		set.row.Values()[i] = row.Values()[set.pathIndex[path]]
	}
	set.row.SetTimestamp(row.Timestamp())
	return set.row, nil
}

func (set *MergeQueryDataSet) Close() {
	set.reader.Close()
}
