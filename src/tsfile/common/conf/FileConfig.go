package conf

import (
	"bufio"
	"log"
	"os"
	"strconv"
	"strings"
)

const (
	CONFIG_FILE_NAME string = "tsfile-format.properties"

	MAGIC_STRING string = "TsFilev0.8.0"

	// Default bit width of RLE encoding is 8
	RLE_MIN_REPEATED_NUM   int = 8
	RLE_MAX_REPEATED_NUM   int = 0x7FFF
	RLE_MAX_BIT_PACKED_NUM int = 63

	// Gorilla encoding configuration
	FLOAT_LENGTH               int = 32
	FLAOT_LEADING_ZERO_LENGTH  int = 5
	FLOAT_VALUE_LENGTH         int = 6
	DOUBLE_LENGTH              int = 64
	DOUBLE_LEADING_ZERO_LENGTH int = 6
	DOUBLE_VALUE_LENGTH        int = 7
)

// Memory size threshold for flushing to disk or HDFS, default value is 128MB
var GroupSizeInByte int = 128 * 1024 * 1024

// The memory size for each series writer to pack page, default value is 64KB
var PageSizeInByte int = 64 * 1024

// The maximum number of data points in a page, defalut value is 1024 * 1024
var MaxNumberOfPointsInPage int = 1024 * 1024

// Data type for input timestamp, TsFile supports INT32 or INT64
var TimeSeriesDataType string = "INT64"

// Max length limitation of input string
var MaxStringLength int = 128

// Floating-point precision
var FloatPrecision int = 2

// Encoder of time series, TsFile supports TS_2DIFF, PLAIN and RLE(run-length encoding)
var TimeSeriesEncoder string = "TS_2DIFF"

// Encoder of value series. default value is PLAIN.
var ValueEncoder string = "PLAIN"

var Compressor string = "UNCOMPRESSED"

func init() {
	loadProperties()
}

func loadProperties() {
	file, err := os.Open(CONFIG_FILE_NAME)
	if err != nil {
		log.Println("Warn:", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if k, v := loadItem(scanner.Text()); v != "" {
			switch {
			case k == "group_size_in_byte":
				GroupSizeInByte, _ = strconv.Atoi(v)
			case k == "page_size_in_byte":
				PageSizeInByte, _ = strconv.Atoi(v)
			case k == "max_number_of_points_in_page":
				MaxNumberOfPointsInPage, _ = strconv.Atoi(v)
			case k == "time_series_data_type":
				TimeSeriesDataType = v
			case k == "max_string_length":
				MaxStringLength, _ = strconv.Atoi(v)
			case k == "float_precision":
				FloatPrecision, _ = strconv.Atoi(v)
			case k == "time_series_encoder":
				TimeSeriesEncoder = v
			case k == "value_encoder":
				ValueEncoder = v
			case k == "compressor":
				Compressor = v
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func loadItem(text string) (key string, value string) {
	text = strings.TrimSpace(text)
	if strings.HasPrefix(text, "#") {
		return "", ""
	} else if result := strings.Split(text, "="); len(result) > 1 {
		return strings.TrimSpace(result[0]), strings.TrimSpace(result[1])
	}

	return key, ""

}
