package constant

import (
	"strings"
)

type CompressionType int16

const (
	UNCOMPRESSED CompressionType = 0
	SNAPPY       CompressionType = 1
	GZIP         CompressionType = 2
	LZO          CompressionType = 3
	SDT          CompressionType = 4
	PAA          CompressionType = 5
	PLA          CompressionType = 6
)

func (compression CompressionType) FindByShortName(name string) CompressionType {
	if name == "" {
		return UNCOMPRESSED
	}
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "UNCOMPRESSED":
		return UNCOMPRESSED
	case "SNAPPY":
		return SNAPPY
	case "GZIP":
		return GZIP
	case "LZO":
		return LZO
	case "SDT":
		return SDT
	case "PAA":
		return PAA
	case "PLA":
		return PLA
	default:
		panic("CompressionTypeNotSupportedException")
	}
}

func (compression CompressionType) GetExtension() string {
	switch compression {
	case UNCOMPRESSED:
		return ""
	case SNAPPY:
		return ".snappy"
	case GZIP:
		return ".gz"
	case LZO:
		return ".lzo"
	case SDT:
		return ".sdt"
	case PAA:
		return ".paa"
	case PLA:
		return ".pla"
	default:
		return ""
	}
}
