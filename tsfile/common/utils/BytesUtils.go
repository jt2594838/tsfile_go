package utils

func BytsfileoBool(bytes []byte) bool {
	return (bytes[0] == 0)
}

//func BytsfileoFloat(bytes []byte) float32 {
//    var l int32;
//    l = b[3];
//    l &= 0xff;
//    l |= ((long) b[2] << 8);
//    l &= 0xffff;
//    l |= ((long) b[1] << 16);
//    l &= 0xffffff;
//    l |= ((long) b[0] << 24);

//    return Float.intBitsToFloat(l);
//}

//func BytsfileoDouble(bytes []byte) float64 {
//    long value = bytes[7];
//    value &= 0xff;
//    value |= ((long) bytes[6] << 8);
//    value &= 0xffff;
//    value |= ((long) bytes[5] << 16);
//    value &= 0xffffff;
//    value |= ((long) bytes[4] << 24);
//    value &= 0xffffffffL;
//    value |= ((long) bytes[3] << 32);
//    value &= 0xffffffffffL;
//    value |= ((long) bytes[2] << 40);
//    value &= 0xffffffffffffL;
//    value |= ((long) bytes[1] << 48);
//    value &= 0xffffffffffffffL;
//    value |= ((long) bytes[0] << 56);

//    return Double.longBitsToDouble(value);
//}

func BytsfileoShort(bytes []byte) int16 {
	var s int16 = 0
	var s0 int16 = int16(bytes[1] & 0xff)
	var s1 int16 = int16(bytes[0] & 0xff)
	s1 <<= 8
	s = int16(s0 | s1)

	return s
}

func BytsfileoInt(bytes []byte) int32 {
	return int32(bytes[3]&0xFF) |
		int32((bytes[2]&0xFF)<<8) |
		int32((bytes[1]&0xFF)<<16) |
		int32((bytes[0]&0xFF)<<24)
}

func BytsfileoLong(bytes []byte) int64 {
	var num int64 = 0

	for ix := 0; ix < 8; ix++ {
		num <<= 8
		num |= int64(bytes[ix] & 0xff)
	}

	return num
}
