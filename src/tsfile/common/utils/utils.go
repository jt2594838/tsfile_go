package utils

import (
	"container/list"
	"errors"
	"math"
	"reflect"
	"time"
	"tsfile/common/log"
)

/**
 * @Package Name: utils
 * @Author: steven yao
 * @Email:  yhp.linux@gmail.com
 * @Create Date: 18-8-29 上午10:55
 * @Description:
 */

func ListContains(l *list.List, value string) (bool, *list.Element) {
	for e := l.Front(); e != nil; e = e.Next() {
		if e.Value == value {
			return true, e
		}
	}
	return false, nil
}

// find obj in target, target should be map, array, slice
func MapContains(target interface{}, obj interface{}) (bool, error) {
	targetValue := reflect.ValueOf(target)
	switch reflect.TypeOf(target).Kind() {
	case reflect.Slice, reflect.Array:
		for i := 0; i < targetValue.Len(); i++ {
			if targetValue.Index(i).Interface() == obj {
				return true, nil
			}
		}
	case reflect.Map:
		if targetValue.MapIndex(reflect.ValueOf(obj)).IsValid() {
			return true, nil
		}
	}
	return false, errors.New("not in array")
}

func calculateTime() {
	var d time.Duration
	t0 := time.Now()
	log.Info("my log.")
	t1 := time.Now()
	d = t1.Sub(t0)
	log.Info("cost time = %v\n", d)
}

func Round(f float64, n int) float64 {
	pow10_n := math.Pow10(n)
	return math.Trunc((f+0.5/pow10_n)*pow10_n) / pow10_n
}
