package seek

import (
	"tsfile/timeseries/read/reader/impl/basic"
	"tsfile/timeseries/read/datatype"
)

type SeekableRowReader struct {
	basic.RowRecordReader

	current *datatype.RowRecord
}

func (r *SeekableRowReader) Next() *datatype.RowRecord{
	r.current = r.RowRecordReader.Next()
	return r.current
}


func (r *SeekableRowReader) Current() *datatype.RowRecord {
	return r.current
}

func (r *SeekableRowReader) Seek(timestamp int64) bool {
	if r.current == nil {
		if r.HasNext() {
			r.Next()
		} else {
			return false
		}
	}
	for {
		if r.current.Timestamp() < timestamp {
			if r.HasNext() {
				r.Next()
				continue
			} else {
				return false
			}
		} else if r.current.Timestamp() == timestamp {
			return true
		} else {
			return false
		}
	}
}

