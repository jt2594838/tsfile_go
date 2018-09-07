package utils

import (
	_ "log"
)

// get one bit in input byte. the offset is from low to high and start with 0
// e.g.<br>
// data:16(00010000), if offset is 4, return 1(000 "1" 0000) if offset is 7, return 0("0" 0010000)
func GetByteN(data byte, offset int) int {
	offset %= 8

	if (data & (1 << uint(7-offset))) != 0 {
		return 1
	} else {
		return 0
	}
}

// set one bit in input integer. the offset is from low to high and start with index 0
// e.g.<br>
// data:1000({00000000 00000000 00000011 11101000}),
// if offset is 4, value is 1, return 1016({00000000 00000000 00000011 111 "1" 1000})
// if offset is 9, value is 0 return 488({00000000 00000000 000000 "0" 1 11101000})
// if offset is 0, value is 0 return 1000(no change)
func SetIntN(data int, offset int, value int) int {
	offset %= 32

	if value == 1 {
		return (data | (1 << uint(offset)))
	} else {
		return (data & ^(1 << uint(offset)))
	}
}

// set one bit in input long. the offset is from low to high and start with index 0
func SetLongN(data int64, offset int, value int) int64 {
	offset %= 64

	if value == 1 {
		return (data | (1 << uint(offset)))
	} else {
		return (data & ^(1 << uint(offset)))
	}
}

// given a byte array, read width bits from specified position bits and convert it to an integer
func BytesToInt(result []byte, pos int, width int) int {
	var value int = 0
	var temp int = 0

	for i := 0; i < width; i++ {
		temp = (pos + width - 1 - i) / 8
		value = SetIntN(value, i, GetByteN(result[temp], pos+width-1-i))
	}
	return value
}

// given a byte array, read width bits from specified pos bits and convert it to an long
func BytesToLong(data []byte, pos int, width int) int64 {
	var value int64 = 0
	var temp int = 0

	for i := 0; i < width; i++ {
		temp = (pos + width - 1 - i) / 8
		value = SetLongN(value, i, GetByteN(data[temp], pos+width-1-i))
	}

	return value
}
