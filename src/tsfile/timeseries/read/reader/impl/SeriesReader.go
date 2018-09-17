package impl

import (
	"tsfile/timeseries/read"
	"tsfile/timeseries/read/datatype"
)

type SeriesReader struct {
	index int
	limit int
	// offsets and sizes of every page of this series in a file
	offsets    []int64
	sizes      []int
	fileReader *read.TsFileSequenceReader
	pageReader *PageDataReader
}

func (r *SeriesReader) HasNext() bool {
	if r.pageReader != nil {
		if r.pageReader.HasNext() {
			return true
		} else if r.index < r.limit {
			r.nextPageReader()
			return r.HasNext()
		} else {
			return false
		}
	} else if r.index < r.limit {
		r.nextPageReader()
		return r.HasNext()
	}
	return false
}

func (r *SeriesReader) Next() datatype.TimeValuePair {
	return r.pageReader.Next()
}

func (r *SeriesReader) Close() {
	r.pageReader.Close()
	r.pageReader = nil
	r.index = r.limit
	r.fileReader = nil
}

func NewSeriesReader(offsets []int64, sizes []int, reader *read.TsFileSequenceReader) *SeriesReader {
	return &SeriesReader{-1, len(offsets), offsets, sizes, reader, nil}
}

func (r *SeriesReader) nextPageReader() {
	r.index ++
	r.pageReader = new(PageDataReader)
	r.pageReader.Read(r.fileReader.ReadRaw(r.offsets[r.index], r.sizes[r.index]))
}