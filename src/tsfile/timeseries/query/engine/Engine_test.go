package engine

import (
	"fmt"
	"testing"
	"tsfile/timeseries/filter"
	"tsfile/timeseries/filter/operator"
	"tsfile/timeseries/query"
	"tsfile/timeseries/read"
	"tsfile/timeseries/write/tsFileWriter"
	"tsfile/timeseries/write/sensorDescriptor"
	"tsfile/common/constant"
	"errors"
)

var tempFilePath = "temp_TsFile"
var series = []string{"root.d0.s0", "root.d0.s1", "root.d1.s0"}

func prepareTsFile() (err error) {
	/*
		Assumed data layout:
		root.d0.s0 : [1,1], [2,2], [3,3], [4,4], [5,5]
		root.d0.s1 : [1,5], [2,4],        [4,3], [5,2], [6,1]
		root.d1.s0 :               [3,3], [4,4], [5,5]
	*/
	d0s0_time := []int64{1,2,3,4,5}
	d0s0_val := []int32{1,2,3,4,5}
	d0s1_time := []int64{1,2,4,5,6}
	d0s1_val := []int32{5,4,3,2,1}
	d1s0_time := []int64{3,4,5}
	d1s0_val := []int32{3,4,5}

	writer, err := tsFileWriter.NewTsFileWriter(tempFilePath)
	if err != nil {
		return err
	}

	des, _ := sensorDescriptor.New("s0", constant.INT32, constant.RLE)
	writer.AddSensor(des);
	des, _ = sensorDescriptor.New("s1", constant.INT32, constant.RLE)
	writer.AddSensor(des);

	for i, t := range d0s0_time {
		record, _ := tsFileWriter.NewTsRecordUseTimestamp(t, "root.d0")
		pt, _ := tsFileWriter.NewInt("s0", constant.INT32, d0s0_val[i])
		record.AddTuple(pt)
		writer.Write(*record)
	}
	for i, t := range d0s1_time {
		record, _ := tsFileWriter.NewTsRecordUseTimestamp(t, "root.d0")
		pt, _ := tsFileWriter.NewInt("s1", constant.INT32, d0s1_val[i])
		record.AddTuple(pt)
		writer.Write(*record)
	}
	for i, t := range d1s0_time {
		record, _ := tsFileWriter.NewTsRecordUseTimestamp(t, "root.d1")
		pt, _ := tsFileWriter.NewInt("s0", constant.INT32, d1s0_val[i])
		record.AddTuple(pt)
		writer.Write(*record)
	}

	if !writer.Close() {
		return errors.New("Cannot close the the TsFile")
	}
	return nil
}

func TestEngine(t *testing.T) {

	err := prepareTsFile()
	if err != nil {
		t.Fatal(err)
	}

	f := new(read.TsFileSequenceReader)
	f.Open(tempFilePath)
	engine := new(Engine)
	engine.Open(f)
	defer func() {
		engine.Close()
		f.Close()
	}()

	// test a non-existing series
	paths := []string{"not a series"}
	exp := new(query.QueryExpression)
	exp.SetSelectPaths(paths)
	dataSet := engine.Query(exp)
	if dataSet.HasNext() {
		t.Fatal("This timeseries should not present in this file")
	}

	// test selecting a series without conditions
	paths = []string{series[0]}
	exp.SetSelectPaths(paths)
	dataSet = engine.Query(exp)
	cnt := int32(0)
	for dataSet.HasNext() {
		record, err := dataSet.Next()
		if err != nil {
			t.Fatal(err)
		}
		cnt++
		checkPath(paths, record.Paths(), t)
		if record.Timestamp() != int64(cnt) || record.Values()[0].(int32) != cnt {
			t.Fatal(fmt.Sprintf("Expected [%d, %d] got %v", cnt, cnt, record))
		}
	}

	// test an existing series but no value satisfies the given condition
	paths = []string{series[0]}
	var filt filter.Filter = filter.NewRowRecordValFilter(series[0], &operator.IntGtFilter{5})
	exp.SetSelectPaths(paths)
	exp.SetFilter(filt)
	dataSet = engine.Query(exp)
	if dataSet.HasNext() {
		t.Fatal("This timeseries should not have any value > 5")
	}

	// test an existing series with some satisfying values
	paths = []string{series[0]}
	filt = filter.NewRowRecordValFilter(series[0], &operator.IntLtEqFilter{3})
	exp.SetSelectPaths(paths)
	exp.SetFilter(filt)
	dataSet = engine.Query(exp)
	cnt = int32(0)
	for dataSet.HasNext() {
		record, err := dataSet.Next()
		if err != nil {
			t.Fatal(err)
		}
		cnt++
		checkPath(paths, record.Paths(), t)
		if record.Timestamp() != int64(cnt) || record.Values()[0].(int32) != cnt {
			t.Fatal(fmt.Sprintf("Expected [%d, %d] got %v", cnt, cnt, record))
		}
	}
	if cnt > 3 {
		t.Fatal(fmt.Sprintf("More values than expected are returned, expected 3 got %d", cnt))
	}

	// test selecting multiple series without conditions
	paths = []string{series[0], series[1]}
	exp.SetSelectPaths(paths)
	dataSet = engine.Query(exp)
	cnt = int32(0)
	var s0Vals []interface{}
	s0Vals = append(s0Vals, 1, 2, 3, 4, 5, nil)
	var s1Vals []interface{}
	s1Vals = append(s1Vals,5, 4, nil, 3, 2, 1 )
	for dataSet.HasNext() {
		record, err := dataSet.Next()
		if err != nil {
			t.Fatal(err)
		}
		checkPath(paths, record.Paths(), t)
		if record.Timestamp() != int64(cnt+1) ||
			record.Values()[0].(int32) != s0Vals[cnt] ||
			record.Values()[1].(int32) != s1Vals[cnt] {
			t.Fatal(fmt.Sprintf("Expected [%d, %d, %d] got %v", cnt+1, s0Vals[cnt], s1Vals[cnt], record))
		}
		cnt++
	}

	// test selecting multiple series with conditions that can't be satisfied
	paths = []string{series[0], series[1]}
	filt = &filter.RowRecordTimeFilter{&operator.LongGtEqFilter{10}}
	exp.SetSelectPaths(paths)
	exp.SetFilter(filt)
	dataSet = engine.Query(exp)
	if dataSet.HasNext() {
		t.Fatal("This timeseries should not have any time > 10")
	}

	// test selecting multiple series with satisfiable conditions
	// and the condition path is among the select paths
	paths = []string{series[0], series[1]}
	filt = filter.NewRowRecordValFilter(series[0], &operator.IntGtEqFilter{4})
	exp.SetSelectPaths(paths)
	exp.SetConditionPaths([]string{series[0]})
	exp.SetFilter(filt)
	dataSet = engine.Query(exp)
	cnt = int32(0)
	s0Vals = nil
	s0Vals = append(s0Vals, 4, 5)
	s1Vals = nil
	s1Vals = append(s1Vals, 3, 2)
	for dataSet.HasNext() {
		record, err := dataSet.Next()
		if err != nil {
			t.Fatal(err)
		}
		checkPath(paths, record.Paths(), t)
		if record.Timestamp() != int64(cnt+4) ||
			record.Values()[0].(int32) != s0Vals[cnt] ||
			record.Values()[1].(int32) != s1Vals[cnt] {
			t.Fatal(fmt.Sprintf("Expected [%d, %d, %d] got %v", cnt+4, s0Vals[cnt], s1Vals[cnt], record))
		}
		cnt++
	}

	// test selecting multiple series with satisfiable conditions
	// and the condition path is outside the select paths
	paths = []string{series[0], series[1]}
	filt = filter.NewRowRecordValFilter(series[2], &operator.IntGtEqFilter{4})
	exp.SetConditionPaths([]string{series[2]})
	exp.SetSelectPaths(paths)
	exp.SetFilter(filt)
	dataSet = engine.Query(exp)
	cnt = int32(0)
	s0Vals = nil
	s0Vals = append(s0Vals, 4, 5)
	s1Vals = nil
	s1Vals = append(s1Vals, 3, 2)
	for dataSet.HasNext() {
		record, err := dataSet.Next()
		if err != nil {
			t.Fatal(err)
		}
		checkPath(paths, record.Paths(), t)
		if record.Timestamp() != int64(cnt+4) ||
			record.Values()[0].(int32) != s0Vals[cnt] ||
			record.Values()[1].(int32) != s1Vals[cnt] {
			t.Fatal(fmt.Sprintf("Expected [%d, %d, %d] got %v", cnt+4, s0Vals[cnt], s1Vals[cnt], record))
		}
		cnt++
	}

	// test selecting multiple series with satisfiable conditions
	// and the condition paths share some common paths with the select paths
	paths = []string{series[0], series[1]}
	filt = &operator.AndFilter{[]filter.Filter{filter.NewRowRecordValFilter(series[2], &operator.IntGtEqFilter{4}),
		filter.NewRowRecordValFilter(series[1], &operator.IntGtEqFilter{3})}}

	exp.SetConditionPaths([]string{series[2]})
	exp.SetSelectPaths(paths)
	exp.SetFilter(filt)
	dataSet = engine.Query(exp)
	cnt = int32(0)
	s0Vals = nil
	s0Vals = append(s0Vals, 4)
	s1Vals = nil
	s1Vals = append(s1Vals, 3)
	for dataSet.HasNext() {
		record, err := dataSet.Next()
		if err != nil {
			t.Fatal(err)
		}
		checkPath(paths, record.Paths(), t)
		if record.Timestamp() != int64(cnt+4) ||
			record.Values()[0].(int32) != s0Vals[cnt] ||
			record.Values()[1].(int32) != s1Vals[cnt] {
			t.Fatal(fmt.Sprintf("Expected [%d, %d, %d] got %v", cnt+4, s0Vals[cnt], s1Vals[cnt], record))
		}
		cnt++
	}
}

func checkPath(pathA []string, pathB []string, t *testing.T) {
	if len(pathA) != len(pathB) {
		t.Fatal("SelectPaths not consistent")
	}
	for i, _ := range pathA {
		if pathA[i] != pathB[i] {
			t.Fatal("SelectPaths not consistent")
		}
	}
}
