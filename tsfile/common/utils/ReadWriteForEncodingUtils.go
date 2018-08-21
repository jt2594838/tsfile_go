package utils

func ReadUnsignedVarInt(data []byte) int {
	var i uint = 0
	var b byte = 0
	var value = 0

	for _, v := range data {
		if (v & 0x80) == 0 {
			b = v
			break
		} else {
			value |= int((v & 0x7F) << i)
			i += 7
		}
	}

	return value | int(b<<i)
}
