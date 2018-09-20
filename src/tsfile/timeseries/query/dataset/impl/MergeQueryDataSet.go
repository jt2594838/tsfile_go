package impl

import (
	"tsfile/timeseries/read/reader"
	"tsfile/timeseries/filter"
	"tsfile/timeseries/read/datatype"
	"tsfile/common/utils"
	"tsfile/timeseries/read/reader/impl/basic"
)


// MergeDataSet merges paths in the select clause and where clause together and constructs rows using all the paths
// and applies the filter on each row.
type MergeQueryDataSet struct {
	reader reader.IRowRecordReader

	row *datatype.RowRecord
	selectPaths []string
	pathIndex map[string]int
}

func NewQueryDataSet(selectPaths []string, conditionPaths []string, readerMap map[string]reader.ISeriesReader, filter filter.Filter) *MergeQueryDataSet {
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

func (set *MergeQueryDataSet) Next() *datatype.RowRecord {
	row := set.reader.Next()
	for i, path := range set.selectPaths {
		set.row.Values()[i] = row.Values()[set.pathIndex[path]]
	}
	set.row.SetTimestamp(row.Timestamp())
	return set.row
}

func (set *MergeQueryDataSet) Close()  {
	set.reader.Close()
}