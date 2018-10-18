package encoder

import (
	"bytes"
	"tsfile/common/conf"
	"tsfile/common/constant"
	"tsfile/common/log"
	"tsfile/common/utils"
	"tsfile/encoding/bitpacking"
)

/**
 * @Package Name: encoder
 * @Author: steven yao
 * @Email:  yhthis.linux@gmail.com
 * @Create Date: 18-10-10 下午2:12
 * @Description:
 */
/*
type RleEncoder struct {
	tsDataType   int16
	endianType   int16
	encodeEndian int16
	valueCount   int
}
*/

/*
public EndianType endianType;
     * we save all value in a list and calculate its bitwidth
    protected List<T> values;
     * the bit width used for bit-packing and rle
    protected int bitWidth;
     * for a given value now buffered, how many times it occurs
    protected int repeatCount;
     * the number of group which using bit packing, it is saved in header
    protected int bitPackedGroupCount;
     * the number of buffered value in array
    protected int numBufferedValues;
     * we will write all bytes using bit-packing to OutputStream once. Before that, all bytes are
     * saved in list
    protected List<byte[]> bytesBuffer;
     * flag which indicate encoding mode false -- rle true -- bit-packing
    protected boolean isBitPackRun;
     * previous value written, used to detect repeated values
    protected T preValue;
     * array to buffer values temporarily
    protected T[] bufferedValues;

    protected boolean isBitWidthSaved;
     * output stream to buffer {@code <bitwidth> <encoded-data>}
    protected ByteArrayOutputStream byteCache;
    protected TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
*/

type RleEncoder struct {
    tsDataType   constant.TSDataType
	endianType   int16
	encodeEndian int16
    values_32 			[]int32
    values_64 			[]int64
    bitWidth			int
    repeatCount			int
    bitPackedGroupCount  	int
    bytesBuffer			[]byte
    isBitPackRun		bool
    preValue_32			int32
    preValue_64			int64
    isBitWidthSaved		bool
    byteCache			*bytes.Buffer
    packer_32 			*bitpacking.IntPacker
    packer_64 			*bitpacking.LongPacker
    numBufferedValues		int
    bufferedValues_32		[]int32;
    bufferedValues_64		[]int64;
}


func (this *RleEncoder) Encode(value interface{}, buffer *bytes.Buffer) {
	log.Info("enter RleEncoder!!")
	switch {
	case this.tsDataType == (constant.BOOLEAN):
		if data, ok := value.(bool); ok {
			this.EncBool(data, buffer)
		}
	case this.tsDataType == (constant.INT32):
		if data, ok := value.(int32); ok {
			this.EncInt32(data, buffer)
		}
	case this.tsDataType == (constant.INT64):
		if data, ok := value.(int64); ok {
			this.EncInt64(data, buffer)
		}
/*
		if data, ok := value.(int64); ok {
			if this.valueCount == -1 {
				aa := []byte{24}
				buffer.Write(aa)
				this.EncInt64(data, buffer)
				this.EncInt64(data, buffer)
				this.EncInt64(data, buffer)
				this.valueCount += 1
			}
			if this.valueCount == conf.DeltaBlockSize {
				this.EncInt64(data, buffer)
				this.EncInt64(data, buffer)
				this.EncInt64(data, buffer)
				this.valueCount = 0
			} else {
				this.valueCount += 1
			}

		}
*/
	default:
		log.Error("invalid input encode type: %d", this.tsDataType)
	}
	return
}

func (this *RleEncoder) EncBool(value bool, buffer *bytes.Buffer) {
        if (value) {
		this.EncInt32(1, buffer);
	} else {
		this.EncInt32(0, buffer);
	}
}


func (this *RleEncoder) EncInt32(value int32, buffer *bytes.Buffer) {
	this.values_32 = append(this.values_32,value)
}

func (this *RleEncoder) EncInt64(value int64, buffer *bytes.Buffer) {
	this.values_64 = append(this.values_64,value)
	//this.values_64.add(value);
}

func numberOfLeadingZerosInt(i int)(int){
        if (i == 0) {
            return 32;
	}
        var n = 1;
        if (i >> 16 == 0) { n += 16; i <<= 16; }
        if (i >> 24 == 0) { n +=  8; i <<=  8; }
        if (i >> 28 == 0) { n +=  4; i <<=  4; }
        if (i >> 30 == 0) { n +=  2; i <<=  2; }
        n -= i >> 31;
        return n;
}

func numberOfLeadingZerosLong(i int64)(int) {
	var x, y int;
	if (i == 0){
	return 64;
	}
	n := 63;
	y = int(i);
	//y = i;
	if (y != 0) {
	 n = n -32; x = y; 
	} else{ 
		x = (int)(i>>32);
	}
y = x <<16; if (y != 0) { n = n -16; x = y; }
y = x << 8; if (y != 0) { n = n - 8; x = y; }
y = x << 4; if (y != 0) { n = n - 4; x = y; }
y = x << 2; if (y != 0) { n = n - 2; x = y; }
return n - ((x << 1) >> 31);
}

func getIntMaxBitWidth(list []int32)(int32) {
/*
        var max = 1;
        for (int num : list) {
            int bitWidth = 32 - numberOfLeadingZerosInt(num);
	    if(bitWidth > max) {
		max = bitWidth
		}
            //max = bitWidth > max ? bitWidth: max;
        }
        return max;
*/
	return 0;
}

func getLongMaxBitWidth(list []int64)(int) {
        max := 1;
        //for (int64 num : list) {
        for _,num := range list {
            bitWidth := 64 - numberOfLeadingZerosLong(num);
	    if(bitWidth > max) {
		max = bitWidth
		}
            //max = bitWidth > max ? bitWidth : max;
        }
        return max;
    }


func (this *RleEncoder)endPreviousBitPackedRun(lastBitPackedNum int32) {
        if (!this.isBitPackRun) {
            return;
        }
        bitPackHeader := (byte) ((this.bitPackedGroupCount << 1) | 1);
        this.byteCache.Write([]byte{bitPackHeader});
        this.byteCache.Write(utils.Int32ToByte(lastBitPackedNum,this.encodeEndian));
        //for (byte[] bytes : bytesBuffer) {
        for _,bytes := range this.bytesBuffer {
            this.byteCache.Write([]byte{bytes});
        }
	this.bytesBuffer  = this.bytesBuffer[0:0]
        this.isBitPackRun = false;
        this.bitPackedGroupCount = 0;
    }


//func (this *RleEncoder)writeRleRun()(error) {
func (this *RleEncoder)writeRleRun()() {
	switch this.tsDataType {
	case (constant.BOOLEAN):
	case (constant.INT32):

        this.endPreviousBitPackedRun(int32(conf.RLE_MIN_REPEATED_NUM));
        utils.WriteUnsignedVarInt(int32(this.repeatCount << 1), this.byteCache);
        utils.WriteIntLittleEndianPaddedOnBitWidth((this.preValue_32), this.byteCache, this.bitWidth);
        this.repeatCount = 0;
        this.numBufferedValues = 0;
	break;
	case (constant.INT64):
        this.endPreviousBitPackedRun(int32(conf.RLE_MIN_REPEATED_NUM));
        utils.WriteUnsignedVarInt(int32(this.repeatCount << 1), this.byteCache);
        utils.WriteLongLittleEndianPaddedOnBitWidth(this.preValue_64, this.byteCache, this.bitWidth);
        this.repeatCount = 0;
        this.numBufferedValues = 0;
    }
}



func (this *RleEncoder)convertBuffer() {
	bytes := make([]byte,this.bitWidth)
	switch this.tsDataType {
	case (constant.BOOLEAN):
	case (constant.INT32):
	tmpBuffer := make([]int32,conf.RLE_MIN_REPEATED_NUM)
        for i := 0; i < conf.RLE_MIN_REPEATED_NUM; i++ {
            tmpBuffer[i] = (this.bufferedValues_32[i])
        }
        this.packer_32.Pack8Values(tmpBuffer, 0, bytes);
        // we'll not write bit-packing group to OutputStream immediately
        // we buffer them in list
        this.bytesBuffer = append(this.bytesBuffer,bytes...)
	break;
	case (constant.INT64):
	tmpBuffer := make([]int64,conf.RLE_MIN_REPEATED_NUM)
        for i := 0; i < conf.RLE_MIN_REPEATED_NUM; i++ {
            tmpBuffer[i] = ( this.bufferedValues_64[i])
        }
        this.packer_64.Pack8Values(tmpBuffer, 0, bytes);
        this.bytesBuffer = append(this.bytesBuffer,bytes...)
        //this.bytesBuffer.add(bytes);
    }
}

func (this *RleEncoder)writeOrAppendBitPackedRun() {
        if (this.bitPackedGroupCount >= conf.RLE_MAX_BIT_PACKED_NUM) {
            this.endPreviousBitPackedRun(int32(conf.RLE_MIN_REPEATED_NUM));
        }
        if (!this.isBitPackRun) {
            this.isBitPackRun = true;
        }

        this.convertBuffer();

        this.numBufferedValues = 0;
        this.repeatCount = 0;
        this.bitPackedGroupCount = this.bitPackedGroupCount+1
}


func (this *RleEncoder)encodeValue(v int64){
        if (!this.isBitWidthSaved) {
            this.byteCache.Write(utils.Int32ToByte(int32(this.bitWidth),this.encodeEndian));
            this.isBitWidthSaved = true;
        }
	b := false
	is32 := false
	if(this.tsDataType  == constant.INT32 || this.tsDataType == constant.BOOLEAN) {
		is32 = true;
	} else {
		is32 = false;
	}
	if(is32 && int32(v) == (this.preValue_32)) {
		b = true;
	}
	if(!is32 && int64(v) == (this.preValue_64)) {
		b = true;
	}
        if (b) {
            this.repeatCount++;
            if (this.repeatCount >= conf.RLE_MIN_REPEATED_NUM && this.repeatCount <= conf.RLE_MAX_REPEATED_NUM) {
                // value occurs more than RLE_MIN_REPEATED_NUM times but less than EncodingConfig.RLE_MAX_REPEATED_NUM
                // we'll use rle, so just keep on counting repeats for now
                // we'll write current value to OutputStream when we encounter a different value
                return;
            } else if (this.repeatCount == conf.RLE_MAX_REPEATED_NUM + 1) {
                // value occurs more than EncodingConfig.RLE_MAX_REPEATED_NUM
                // we'll write current rle run to stream and keep on counting current value
                this.repeatCount = conf.RLE_MAX_REPEATED_NUM;
                    this.writeRleRun();
              this.repeatCount = 1;
		if(is32) {
              		this.preValue_32 = int32(v)
		}else {
              		this.preValue_64 = v
		}
            }
        } else {
            if (this.repeatCount >= conf.RLE_MIN_REPEATED_NUM) {
                    this.writeRleRun();
            }
            this.repeatCount = 1;
		if(is32) {
              		this.preValue_32 = int32(v)
		}else {
              		this.preValue_64 = v
		}
        }
		if(is32) {
        this.bufferedValues_32[this.numBufferedValues] = int32(v);
		}else {
        this.bufferedValues_64[this.numBufferedValues] = v;
		}
        this.numBufferedValues++;
        if (this.numBufferedValues == conf.RLE_MIN_REPEATED_NUM) {
            this.writeOrAppendBitPackedRun();
        }
    }


func (this *RleEncoder)clearBuffer() {
        for i := this.numBufferedValues; i < conf.RLE_MIN_REPEATED_NUM; i++  {
		switch this.tsDataType {
		case (constant.BOOLEAN):
		case (constant.INT32):
            		this.bufferedValues_32[i] = 0;
			break;
		case (constant.INT64):
            		this.bufferedValues_64[i] = 0;
			break;
        }
    }
}


func (this *RleEncoder)reset() {
	this.numBufferedValues = 0;
        this.repeatCount = 0;
        this.bitPackedGroupCount = 0;
        this.bytesBuffer = this.bytesBuffer[0:0]
        this.isBitPackRun = false;
        this.isBitWidthSaved = false;
        this.byteCache.Reset()// = this.byteCache[0:0]
	switch this.tsDataType {
	case (constant.BOOLEAN):
	case (constant.INT32):
        	this.values_32= this.values_32[0:0]
        	this.preValue_32 = 0//this.preValue_32[0:0]
		break;
		case (constant.INT64):
        	this.values_64= this.values_64[0:0]
        	this.preValue_64 = int64(0)//this.preValue_64[0:0]
			break;
	}
}


func (this *RleEncoder) flush(buffer *bytes.Buffer) {
        lastBitPackedNum := int32(this.numBufferedValues)
        if (this.repeatCount >= conf.RLE_MIN_REPEATED_NUM) {
        	this.writeRleRun();
        } else if (this.numBufferedValues > 0) {
            this.clearBuffer();
            this.writeOrAppendBitPackedRun();
            this.endPreviousBitPackedRun(lastBitPackedNum);
        } else {
            this.endPreviousBitPackedRun(int32(conf.RLE_MIN_REPEATED_NUM))
        }
        //write length
        utils.WriteUnsignedVarInt(int32(this.byteCache.Len()), buffer);
        buffer.Write(this.byteCache.Bytes())
        //this.byteCache.writeTo(buffer);
        this.reset();

}

func (this *RleEncoder) Flush(buffer *bytes.Buffer) {
	this.bitWidth = int(getIntMaxBitWidth(this.values_32));
	this.packer_32 = &bitpacking.IntPacker{BitWidth:int( this.bitWidth)}
	switch this.tsDataType {
	case (constant.BOOLEAN):
	case (constant.INT32):
		for _,v := range this.values_32 {
            		this.encodeValue(int64(v))
        	}
		break;
	case (constant.INT64):
		for _,v := range this.values_64 {
            		this.encodeValue(int64(v))
        	}
		break;
	}
	this.flush(buffer)
}

func (this *RleEncoder) GetMaxByteSize() int64 {
	switch this.tsDataType {
	case (constant.BOOLEAN):
	case (constant.INT32):
		len := len(this.values_32);
        	if (len == 0) {
            		return 0;
        	}
        	groupNum := (len / 8 + 1) / 63 + 1;
		return int64(8 + groupNum * 5 + len * 4);
	case (constant.INT64):
		len := len(this.values_32);
        	if (len == 0) {
            		return 0;
        	}
        	groupNum := (len / 8 + 1) / 63 + 1;
		return int64(8 + groupNum * 5 + len * 8);
	default:
		log.Error("invalid input dataType in plainEncoder. tsDataType: %d", this.tsDataType)
	}
	return 0
}


func (this *RleEncoder) GetOneItemMaxSize() int {
	switch this.tsDataType {
	case (constant.BOOLEAN):
	case (constant.INT32):
		return 45
	case (constant.INT64):
		return 77
	default:
		log.Error("invalid input dataType in plainEncoder. tsDataType: %d", this.tsDataType)
	}
	return 0
}

func NewRleEncoder(tdt constant.TSDataType, endianType int16) (*RleEncoder, error) {
	return &RleEncoder{
		tsDataType:   tdt,
		endianType:   endianType,
		encodeEndian: 1,
	}, nil
}
