package engine

import (
	"tsfile/file/metadata"
	"tsfile/timeseries/read"
)

type Engine struct {
	fileMeta *metadata.FileMetaData
}

func (e *Engine) init(reader *read.TsFileSequenceReader) {
	e.fileMeta = reader.ReadFileMetadata()
}