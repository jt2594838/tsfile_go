package seek

import (
	"tsfile/timeseries/read"
	"tsfile/timeseries/read/datatype"
	"tsfile/timeseries/read/reader/impl/basic"
	"tsfile/file/header"
)

type SeekableSeriesReader struct {
	*basic.SeriesReader

	pageHeaders []header.PageHeader
	current *datatype.TimeValuePair
}

func (r *SeekableSeriesReader) Seek(timestamp int64) bool {

	// seek the page that may contain the given timestamp
	for r.PageIndex < r.PageLimit &&
		! (r.pageHeaders[r.PageIndex].Min_timestamp() <= timestamp && timestamp < r.pageHeaders[r.PageIndex].Max_timestamp() ) {
			r.PageIndex ++
	}
	if r.PageIndex < r.PageLimit {
		r.PageIndex --
		r.nextPageReader()
	} else {
		return false
	}
	// seek within this page
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

func (r *SeekableSeriesReader) Current() *datatype.TimeValuePair {
	return r.current
}


func NewSeekableSeriesReader(offsets []int64, sizes []int, reader *read.TsFileSequenceReader, pageHeaders []header.PageHeader) *SeekableSeriesReader {
	return &SeekableSeriesReader{&basic.SeriesReader{-1, len(offsets),
	offsets, sizes, reader, nil}, pageHeaders, nil}
}

func (r *SeekableSeriesReader) hasNextPageReader() bool {
	return r.PageIndex < r.PageLimit
}

func (r *SeekableSeriesReader) nextPageReader() {
	r.PageIndex ++
	r.PageReader = new(SeekablePageDataReader)
	r.PageReader.Read(r.FileReader.ReadRaw(r.Offsets[r.PageIndex], r.Sizes[r.PageIndex]))
}

func (r *SeekableSeriesReader) HasNext() bool {
	if r.PageReader != nil {
		if r.PageReader.HasNext() {
			return true
		} else if r.PageIndex < r.PageLimit {
			r.nextPageReader()
			return r.HasNext()
		} else {
			return false
		}
	} else if r.PageIndex < r.PageLimit {
		r.nextPageReader()
		return r.HasNext()
	}
	return false
}

func (r *SeekableSeriesReader) Next() *datatype.TimeValuePair {
	if r.PageReader.HasNext() {
		r.current = r.PageReader.Next()
		return r.current
	} else {
		r.nextPageReader()
		return r.Next()
	}
}


