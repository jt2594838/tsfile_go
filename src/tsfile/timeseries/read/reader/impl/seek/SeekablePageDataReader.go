package seek

import (
	"tsfile/timeseries/read/datatype"
	"tsfile/timeseries/read/reader/impl/basic"
)

type SeekablePageDataReader struct {
	*basic.PageDataReader

	current *datatype.TimeValuePair
}

func (r *SeekablePageDataReader) Next() (*datatype.TimeValuePair, error) {
	row, err := r.PageDataReader.Next()
	r.current = row
	return r.current, err
}

func (r *SeekablePageDataReader) Current() *datatype.TimeValuePair {
	return r.current
}

func (r *SeekablePageDataReader) Seek(timestamp int64) bool {
	if r.current == nil {
		if r.HasNext() {
			r.Next()
		} else {
			return false
		}
	}
	for {
		if r.current.Timestamp < timestamp {
			if r.HasNext() {
				r.Next()
				continue
			} else {
				return false
			}
		} else if r.current.Timestamp == timestamp {
			return true
		} else {
			return false
		}
	}
}
