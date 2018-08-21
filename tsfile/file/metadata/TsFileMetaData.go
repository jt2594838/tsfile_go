package metadata

import (
	"encoding/binary"
	_ "log"
)

type TsFileMetaData struct {
	CurrentVersion int

	/**
	 * String for application that wrote this file. This should be in the format <Application> version
	 * <App Version> (build <App Build Hash>). e.g. impala version 1.0 (build SHA-1_hash_code)
	 */
	CreatedBy                        string
	FirstTimeSeriesMetadataOffset    int64 //相对于file metadata开头位置 的offset
	LastTimeSeriesMetadataOffset     int64 //相对于file metadata开头位置 的offset
	FirstTsDeltaObjectMetadataOffset int64 //相对于file metadata开头位置 的offset
	LastTsDeltaObjectMetadataOffset  int64 //相对于file metadata开头位置 的offset
}

func (f *TsFileMetaData) DeserializeFrom(metadata []byte) {
	_ = binary.BigEndian.Uint32(metadata[0:4])

	//        if(size > 0) {
	//            Map<String, TsDeltaObjectMetadata> deltaObjectMap = new HashMap<>();
	//            String key;
	//            TsDeltaObjectMetadata value;
	//            for (int i = 0; i < size; i++) {
	//                key = ReadWriteIOUtils.readString(buffer);
	//                value = TsDeltaObjectMetadata.deserializeFrom(buffer);
	//                deltaObjectMap.put(key, value);
	//            }
	//            fileMetaData.deltaObjectMap = deltaObjectMap;
	//        }

	//        size = ReadWriteIOUtils.readInt(buffer);
	//        if(size > 0) {
	//            List<TimeSeriesMetadata> timeSeriesList = new ArrayList<>();
	//            for (int i = 0; i < size; i++) {
	//                fileMetaData.addTimeSeriesMetaData(ReadWriteIOUtils.readTimeSeriesMetadata(buffer));
	//            }
	//        }

	//        fileMetaData.currentVersion = ReadWriteIOUtils.readInt(buffer);

	//        if(ReadWriteIOUtils.readIsNull(buffer))
	//            fileMetaData.createdBy = ReadWriteIOUtils.readString(buffer);

	//        fileMetaData.firstTimeSeriesMetadataOffset = ReadWriteIOUtils.readLong(buffer);
	//        fileMetaData.lastTimeSeriesMetadataOffset = ReadWriteIOUtils.readLong(buffer);
	//        fileMetaData.firstTsDeltaObjectMetadataOffset = ReadWriteIOUtils.readLong(buffer);
	//        fileMetaData.lastTsDeltaObjectMetadataOffset = ReadWriteIOUtils.readLong(buffer);

	//        return fileMetaData;
}
