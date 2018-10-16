package constant

type TSEncoding int16

const (
	PLAIN            TSEncoding = 0
	PLAIN_DICTIONARY TSEncoding = 1
	RLE              TSEncoding = 2
	DIFF             TSEncoding = 3
	TS_2DIFF         TSEncoding = 4
	BITMAP           TSEncoding = 5
	GORILLA          TSEncoding = 6
)

func GetEncodingByName(name string) TSEncoding {
	switch name {
	case "PLAIN":
		return PLAIN
	case "PLAIN_DICTIONARY":
		return PLAIN_DICTIONARY
	case "RLE":
		return RLE
	case "DIFF":
		return DIFF
	case "TS_2DIFF":
		return TS_2DIFF
	case "BITMAP":
		return BITMAP
	case "GORILLA":
		return GORILLA
	default:
		panic("No encoding found: " + name)
	}
}
