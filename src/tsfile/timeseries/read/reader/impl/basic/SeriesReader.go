package basic

import (
	"tsfile/timeseries/read"
	"tsfile/timeseries/read/datatype"
	"tsfile/timeseries/read/reader"
)

type SeriesReader struct {
	// page PageIndex and PageLimit, indicating the current page and the number of pages
	PageIndex int
	PageLimit int
	// Offsets and Sizes of every page of this series in a file
	Offsets    []int64
	Sizes      []int
	FileReader *read.TsFileSequenceReader
	PageReader reader.TimeValuePairReader
}

func (r *SeriesReader) Read(data []byte) {
	panic("implement me")
}

func (r *SeriesReader) Skip() {
	r.Next()
}

func (r *SeriesReader) HasNext() bool {
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

func (r *SeriesReader) Next() *datatype.TimeValuePair {
	if r.PageReader.HasNext() {
		return r.PageReader.Next()
	} else {
		r.nextPageReader()
		return r.Next()
	}
}

func (r *SeriesReader) Close() {
	r.PageReader.Close()
	r.PageReader = nil
	r.PageIndex = r.PageLimit
	r.FileReader = nil
}

func NewSeriesReader(offsets []int64, sizes []int, reader *read.TsFileSequenceReader) *SeriesReader {
	return &SeriesReader{-1, len(offsets), offsets, sizes, reader, nil}
}

func (r *SeriesReader) hasNextPageReader() bool {
	return r.PageIndex < r.PageLimit
}


func (r *SeriesReader) nextPageReader() {
	r.PageIndex ++
	r.PageReader = new(PageDataReader)
	r.PageReader.Read(r.FileReader.ReadRaw(r.Offsets[r.PageIndex], r.Sizes[r.PageIndex]))
}