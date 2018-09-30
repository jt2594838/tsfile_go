package basic

import (
	"tsfile/timeseries/read"
	"tsfile/timeseries/read/datatype"
	"tsfile/timeseries/read/reader"
	"tsfile/encoding/decoder"
	"tsfile/common/constant"
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
	DType      constant.TSDataType
	Encoding   constant.TSEncoding

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
		} else if r.PageIndex < r.PageLimit -1 {
			r.nextPageReader()
			return r.HasNext()
		} else {
			return false
		}
	} else if r.PageIndex < r.PageLimit - 1 {
		r.nextPageReader()
		return r.HasNext()
	}
	return false
}

func (r *SeriesReader) Next() *datatype.TimeValuePair {
	if r.PageReader.HasNext() {
		ret := r.PageReader.Next()
		return ret
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

func NewSeriesReader(offsets []int64, sizes []int, reader *read.TsFileSequenceReader, dType constant.TSDataType, encoding constant.TSEncoding) *SeriesReader {
	return &SeriesReader{-1, len(offsets), offsets, sizes, reader, nil, dType, encoding}
}

func (r *SeriesReader) hasNextPageReader() bool {
	return r.PageIndex < r.PageLimit
}


func (r *SeriesReader) nextPageReader() {
	r.PageIndex ++
	r.PageReader = &PageDataReader{DataType:r.DType, ValueDecoder:decoder.CreateDecoder(r.Encoding, r.DType),
									TimeDecoder:decoder.CreateDecoder(constant.TS_2DIFF, constant.INT64)}
	r.PageReader.Read(r.FileReader.ReadRaw(r.Offsets[r.PageIndex], r.Sizes[r.PageIndex]))
}