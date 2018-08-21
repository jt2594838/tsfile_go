package read

import (
	//"bufio"
	"encoding/binary"
	"io"

	"log"
	"os"
	"tsfile/common/conf"
	"tsfile/file/header"
	"tsfile/file/metadata"
	"tsfile/file/metadata/enums"
)

type TsFileSequenceReader struct {
	fileName      string
	fin           *os.File
	size          int64
	pos           int64
	metadata_pos  int64
	metadata_size int32
}

func (f *TsFileSequenceReader) Open(file string) {
	f.fileName = file

	var err error
	f.fin, err = os.Open(file)
	if err == nil {
		stat, _ := f.fin.Stat()
		f.size = stat.Size()

		// get matadata pos&size
		buf := make([]byte, 4)
		_, err := f.fin.ReadAt(buf, f.size-int64(len(conf.MAGIC_STRING))-4)
		if err == nil {
			f.metadata_size = int32(binary.BigEndian.Uint32(buf))
			f.metadata_pos = f.size - int64(len(conf.MAGIC_STRING)) - 4 - int64(f.metadata_size)
		}

		// get pointer ready for reading RowGroupHeader
		f.pos, _ = f.fin.Seek(int64(len(conf.MAGIC_STRING)), io.SeekStart)
	} else {
		log.Println("Failed to open file: " + file)
		panic(err)
	}
}

func (f *TsFileSequenceReader) ReadHeadMagic() string {
	size := len(conf.MAGIC_STRING)
	buf := make([]byte, size)
	_, err := f.fin.ReadAt(buf, 0)
	if err == nil {
		return string(buf[:])
	} else {
		panic(err)
	}
}

func (f *TsFileSequenceReader) ReadTailMagic() string {
	size := len(conf.MAGIC_STRING)
	buf := make([]byte, size)
	_, err := f.fin.ReadAt(buf, f.size-int64(size))
	if err == nil {
		return string(buf[:])
	} else {
		panic(err)
	}
}

func (f *TsFileSequenceReader) ReadFileMetadata() *metadata.TsFileMetaData {
	tsMetadata := new(metadata.TsFileMetaData)

	data := make([]byte, f.metadata_size)
	_, err := f.fin.ReadAt(data, f.metadata_pos)
	if err == nil {
		tsMetadata.DeserializeFrom(data)
	} else {
		panic(err)
	}

	return tsMetadata
}

func (f *TsFileSequenceReader) HasNextRowGroup() bool {
	return f.pos < f.metadata_pos
}

func (f *TsFileSequenceReader) ReadRowGroupHeader() *header.RowGroupHeader {
	header := new(header.RowGroupHeader)
	header.DeserializeFrom(f.fin)

	f.pos += int64(header.SerializedSize)

	return header
}

func (f *TsFileSequenceReader) ReadChunkHeader() *header.ChunkHeader {
	header := new(header.ChunkHeader)
	header.DeserializeFrom(f.fin)

	f.pos += int64(header.SerializedSize)

	return header
}

func (f *TsFileSequenceReader) ReadPageHeader() *header.PageHeader {
	header := new(header.PageHeader)
	header.DeserializeFrom(f.fin)

	f.pos += int64(header.SerializedSize)

	return header
}

func (f *TsFileSequenceReader) ReadPage(header *header.PageHeader, compression enums.CompressionType) []byte {
	buf := make([]byte, header.CompressedSize)
	f.fin.Read(buf)

	//    UnCompressor unCompressor = UnCompressor.getUnCompressor(type);
	//    ByteBuffer uncompressedBuffer = ByteBuffer.allocate(header.getUncompressedSize());
	//    switch (type){
	//        case UNCOMPRESSED:
	//            return buffer;
	//        default:
	//            unCompressor.uncompress(buffer.array(), buffer.position(), buffer.remaining(), uncompressedBuffer.array(), 0);
	//            return uncompressedBuffer;
	//    }

	return buf
}

func (f TsFileSequenceReader) Close() {
	f.fin.Close()
}
