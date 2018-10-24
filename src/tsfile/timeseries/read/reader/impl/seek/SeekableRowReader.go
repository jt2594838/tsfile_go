package seek

import (
	"math"
	"tsfile/timeseries/read/datatype"
	"tsfile/timeseries/read/reader"
	"tsfile/common/log"
	"errors"
)

type SeekableRowReader struct {
	paths     []string
	readerMap map[string]reader.ISeekableTimeValuePairReader

	cacheList []*datatype.TimeValuePair
	current   *datatype.RowRecord
	currTime  int64
	exhausted bool
}

func (r *SeekableRowReader) Current() *datatype.RowRecord {
	return r.current
}

func (r *SeekableRowReader) Seek(timestamp int64) bool {
	hasRecord := false
	r.currTime = timestamp
	for i, path := range r.paths {
		if r.readerMap[path].Seek(timestamp) {
			tv := r.readerMap[path].Current()
			r.cacheList[i] = tv
			hasRecord = true
		} else {
			r.cacheList[i] = nil
		}
	}
	r.fillRow()
	return hasRecord
}

func NewSeekableRowReader(paths []string, readerMap map[string]reader.ISeekableTimeValuePairReader) *SeekableRowReader {
	ret := &SeekableRowReader{paths, readerMap, make([]*datatype.TimeValuePair, len(paths)),
		datatype.NewRowRecordWithPaths(paths), math.MaxInt64, false}
	return ret
}

func (r *SeekableRowReader) fillCache() error {
	// try filling the column caches and update the currTime
	for i, path := range r.paths {
		if r.cacheList[i] == nil && r.readerMap[path].HasNext() {
			tv, err := r.readerMap[path].Next()
			if err != nil {
				return err
			}
			r.cacheList[i] = tv
		}
		if r.cacheList[i] != nil && r.cacheList[i].Timestamp < r.currTime {
			r.currTime = r.cacheList[i].Timestamp
		}
	}
	return nil
}

func (r *SeekableRowReader) fillRow() {
	// fill the current cache using column caches
	for i, _ := range r.paths {
		if r.cacheList[i] != nil && r.cacheList[i].Timestamp == r.currTime {
			r.current.Values()[i] = r.cacheList[i].Value
			r.cacheList[i] = nil
		} else {
			r.current.Values()[i] = nil
		}
	}
	r.current.SetTimestamp(r.currTime)
}

func (r *SeekableRowReader) HasNext() bool {
	if r.currTime != math.MaxInt64 {
		return true
	}
	err := r.fillCache()
	if err != nil {
		log.Error("RowRecord exhausted")
		r.exhausted = true
	} else if r.current.Timestamp() == math.MaxInt64 {
		r.exhausted = true
	}
	return !r.exhausted
}

/*
	Notice: The return value is IMMUTABLE because the RowRecord is reused through out the iteration to reduce memory
	overhead. You can only copy the values in the RowRecord instead of copying the pointer of the return value.
*/
func (r *SeekableRowReader) Next() (*datatype.RowRecord, error) {
	if r.exhausted {
		return nil, errors.New("RowRecord exhausted")
	}
	if r.currTime == math.MaxInt64 {
		err := r.fillCache()
		if err != nil {
			r.exhausted = true
			return nil, err
		}
		if r.currTime == math.MaxInt64 {
			r.exhausted = true
			return nil, errors.New("RowRecord exhausted")
		}
	}
	r.fillRow()
	r.currTime = math.MaxInt64

	return r.current, nil
}

func (r *SeekableRowReader) Close() {
	for _, path := range r.paths {
		if r.readerMap[path] != nil {
			r.readerMap[path].Close()
		}
	}
}
