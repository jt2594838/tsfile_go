package encoder

import (
	"bytes"
	//"tsfile/common/conf"
	"tsfile/common/constant"
	//"tsfile/common/log"
	"tsfile/common/utils"
	//"tsfile/encoding/bitpacking"
)

/**
 * @Package Name: encoder
 * @Author: steven yao
 * @Email:  yhthis.linux@gmail.com
 * @Create Date: 18-10-10 下午2:12
 * @Description:
 */
/*
type BitmapEncoder struct {
	tsDataType   int16
	endianType   int16
	encodeEndian int16
	valueCount   int
}
*/


type BitmapEncoder struct {
    	tsDataType   constant.TSDataType
	endianType   int16
	encodeEndian int16
    	values 			[]int32
}


func (this *BitmapEncoder) Encode(value interface{}, buffer *bytes.Buffer) {
	switch this.tsDataType {
        case (constant.INT32):
                if data, ok := value.(int32); ok {
			this.values = append(this.values,data)
                }
		break;
	}

}

func (this *BitmapEncoder) Flush(buffer *bytes.Buffer) {
        var byteCache *bytes.Buffer // = new ByteArrayOutputStream();
        //Set<Integer> valueType = new HashSet<Integer>(values);
        valueType := this.values //make([]int32,this.values...)//new HashSet<Integer>(values);
        len := len(this.values)
        byteNum := (len + 7) / 8;
        if (byteNum == 0) {
            this.reset();
            return;
        }
        //for (int value : valueType) {
	for _,value  := range valueType {
            //buffer []byte = new byte[byteNum];
	   	buffer := make([]byte,byteNum) 
            	for i := 0; i < len; i++ {
                	if (this.values[i] == value) {
                    		index := i / 8;
                    		offset := 7 - (i % 8);
                    		// Encoder use 1 bit in byte to indicate that value appears
                    		//buffer[index] |= ((byte) 1 << offset)
				buffer[index] = (buffer[index] | (byte (1) << uint(offset)))
                	}
            	}
		
		utils.WriteUnsignedVarInt(value,byteCache)
            	byteCache.Write(buffer);
        }
	utils.WriteUnsignedVarInt(int32(byteCache.Len()),buffer)
	utils.WriteUnsignedVarInt(int32(len),buffer)
        buffer.Write(byteCache.Bytes());
        this.reset();

}

func (this *BitmapEncoder) GetMaxByteSize() int64 {
	return int64((4 + 4 + (len(this.values) + 7) / 8 + 4) * len(this.values))
}

func (this *BitmapEncoder) GetOneItemMaxSize() int {
	return 1;
}

func (this *BitmapEncoder)reset() {
	this.values = this.values[0:0]
}

func NewBitmapEncoder(tdt constant.TSDataType, endianType int16) (*BitmapEncoder, error) {
	return &BitmapEncoder{
		tsDataType:   tdt,
		endianType:   endianType,
		encodeEndian: 1,
	}, nil
}
