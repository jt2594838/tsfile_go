package main

import (
	"tsfile/timeseries/read"
	"tsfile/timeseries/query"
	"tsfile/timeseries/filter"
	"tsfile/timeseries/filter/operator"
	"fmt"
	"time"
	"tsfile/timeseries/query/engine"
	"strconv"
)

var filePath = "/Users/koutakashi/codes/tsfile_golang/gen.ts_plain"
var SENSOR_PREFIX = "s"
var DEVICE_PREFIX = "d"
var SEPARATOR = "."

var ptNum = 1000000
var selectNum = 10
var conditionPathNum = 4
var selectRate = 0.1


func TestEnginePerf() {

	f := new(read.TsFileSequenceReader)
	f.Open(filePath)
	engine := new(engine.Engine)
	engine.Open(f)
	defer func() {
		engine.Close()
		f.Close()
	}()

	var paths []string
	for i := 0; i < selectNum; i++ {
		paths = append(paths, DEVICE_PREFIX + strconv.Itoa(i) + SEPARATOR + SENSOR_PREFIX + strconv.Itoa(i))
	}

	// condition paths all in select paths
	exp := new(query.QueryExpression)
	var seriesFilters []filter.Filter
	var conditionPaths []string
	for i := 0; i < conditionPathNum; i++ {
		conditionPath := DEVICE_PREFIX + strconv.Itoa(i) + SEPARATOR + SENSOR_PREFIX + strconv.Itoa(i)
		conditionPaths = append(conditionPaths, conditionPath)
		seriesFilters = append(seriesFilters, filter.NewRowRecordValFilter(conditionPath,
			&operator.DoubleLtFilter{selectRate * float64(ptNum)}))
	}
	var filt filter.Filter = &operator.AndFilter{seriesFilters}
	exp.SetSelectPaths(paths)
	exp.SetConditionPaths(conditionPaths)
	exp.SetFilter(filt)
	startTime := time.Now()
	dataSet := engine.Query(exp)
	cnt := 0
	for dataSet.HasNext() {
		dataSet.Next()
		cnt ++
	}
	fmt.Printf("******Query with Inner Conditions****** %d ms, %d pts\n", time.Now().Sub(startTime).Nanoseconds() / 1000000, cnt)

	// condition paths half in select paths
	conditionPaths = nil
	seriesFilters = nil
	exp = new(query.QueryExpression)
	for i := 0; i < conditionPathNum / 2; i++ {
		conditionPath := DEVICE_PREFIX + strconv.Itoa(i) + SEPARATOR + SENSOR_PREFIX + strconv.Itoa(i)
		conditionPaths = append(conditionPaths, conditionPath)
		seriesFilters = append(seriesFilters, filter.NewRowRecordValFilter(conditionPath,
			&operator.DoubleLtFilter{selectRate * float64(ptNum)}))
	}
	for i := conditionPathNum / 2; i < conditionPathNum; i++ {
		conditionPath := DEVICE_PREFIX + strconv.Itoa(i) + SEPARATOR + SENSOR_PREFIX + strconv.Itoa(i + 1)
		conditionPaths = append(conditionPaths, conditionPath)
		seriesFilters = append(seriesFilters, filter.NewRowRecordValFilter(conditionPath,
			&operator.DoubleLtFilter{selectRate * float64(ptNum)}))
	}
	filt = &operator.AndFilter{seriesFilters}
	exp.SetSelectPaths(paths)
	exp.SetConditionPaths(conditionPaths)
	exp.SetFilter(filt)
	startTime = time.Now()
	dataSet = engine.Query(exp)
	cnt = 0
	for dataSet.HasNext() {
		dataSet.Next()
		cnt ++
	}
	fmt.Printf("******Query with Cross Conditions****** %d ms, %d pts\n", time.Now().Sub(startTime).Nanoseconds() / 1000000, cnt)

	// condition paths none in select paths
	conditionPaths = nil
	seriesFilters = nil
	exp = new(query.QueryExpression)
	for i := 0; i < conditionPathNum; i++ {
		conditionPath := DEVICE_PREFIX + strconv.Itoa(i) + SEPARATOR + SENSOR_PREFIX + strconv.Itoa(i + 1)
		conditionPaths = append(conditionPaths, conditionPath)
		seriesFilters = append(seriesFilters, filter.NewRowRecordValFilter(conditionPath,
			&operator.DoubleLtFilter{selectRate * float64(ptNum)}))
	}
	filt = &operator.AndFilter{seriesFilters}
	exp.SetSelectPaths(paths)
	exp.SetConditionPaths(conditionPaths)
	exp.SetFilter(filt)
	startTime = time.Now()
	dataSet = engine.Query(exp)
	cnt = 0
	for dataSet.HasNext() {
		dataSet.Next()
		cnt ++
	}
	fmt.Printf("******Query with outer Conditions****** %d ms, %d pts\n", time.Now().Sub(startTime).Nanoseconds() / 1000000, cnt)

	// time condition only
	conditionPaths = nil
	exp = new(query.QueryExpression)
	filt = filter.NewRowRecordTimeFilter(&operator.LongLtFilter{int64(selectRate * float64(ptNum))})
	exp.SetSelectPaths(paths)
	exp.SetConditionPaths(conditionPaths)
	exp.SetFilter(filt)
	startTime = time.Now()
	dataSet = engine.Query(exp)
	cnt = 0
	for dataSet.HasNext() {
		dataSet.Next()
		cnt ++
	}
	fmt.Printf("******Query with Time Conditions****** %d ms, %d pts\n", time.Now().Sub(startTime).Nanoseconds() / 1000000, cnt)

	// no condition
	exp = new(query.QueryExpression)
	exp.SetSelectPaths(paths)
	startTime = time.Now()
	dataSet = engine.Query(exp)
	cnt = 0
	for dataSet.HasNext() {
		dataSet.Next()
		cnt ++
	}
	fmt.Printf("******Query with no Conditions****** %d ms, %d pts\n", time.Now().Sub(startTime).Nanoseconds() / 1000000, cnt)

	}

func main() {
	TestEnginePerf()
}