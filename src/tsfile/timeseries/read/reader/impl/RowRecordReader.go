package impl

import (
	"tsfile/timeseries/read/datatype"
	"tsfile/timeseries/read/reader"
	"math"
)

type RowRecordReader struct {
	paths []string
	readerMap map[string]reader.ISeriesReader

	cacheList []*datatype.TimeValuePair
	row       *datatype.RowRecord
	rowCached bool
	currTime int64
}

func NewRecordReader(paths []string, readerMap map[string]reader.ISeriesReader) *RowRecordReader{
	ret := &RowRecordReader{paths:paths, readerMap:readerMap}
	ret.row = datatype.NewRowRecordWithPaths(paths)
	ret.cacheList = make([]*datatype.TimeValuePair, len(paths))
	ret.currTime = math.MaxInt64
	ret.rowCached = false

	return ret
}

func (r *RowRecordReader) HasNext() bool {
	if r.rowCached {
		return true
	}
	// try filling the column caches and update the currTime
	for i, path := range r.paths {
		if r.cacheList[i] == nil && r.readerMap[path].HasNext() {
			tv := r.readerMap[path].Next()
			r.cacheList[i] = &tv
			if tv.Timestamp < r.currTime {
				r.currTime =  tv.Timestamp
			}
		}
	}
	// fill the row cache using column caches
	for i, _ := range r.paths {
		if r.cacheList[i] != nil && r.cacheList[i].Timestamp == r.currTime {
			r.row.Values()[i] = r.cacheList[i].Value
			r.cacheList[i] = nil
			r.rowCached = true
		} else  {
			r.row.Values()[i] = nil
		}
	}
	r.row.SetTimestamp(r.currTime)
	return r.rowCached
}

/*
	Notice: The return value is IMMUTABLE because the RowRecord is reused through out the iteration to reduce memory
	overhead. You can only copy the values in the RowRecord instead of copying the pointer of the return value.
 */
func (r *RowRecordReader) Next() *datatype.RowRecord {
	r.rowCached = false
	r.currTime = math.MaxInt64
	return r.row
}

func (r *RowRecordReader) Close() {
	for _, path := range r.paths {
		if r.readerMap[path] != nil {
			r.readerMap[path].Close()
		}
	}
}
