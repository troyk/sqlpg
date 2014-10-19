package sqlpg

import (
	"reflect"
)

func IsEmpty(v interface{}) bool {
	if v == nil {
		return true
	}

	t := reflect.TypeOf(v)

	switch t.Kind() {
	case reflect.Array, reflect.Slice, reflect.Map, reflect.Chan, reflect.String:
		if reflect.ValueOf(v).Len() == 0 {
			return true
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if reflect.ValueOf(v).Int() == 0 {
			return true
		}
	default:
		if reflect.DeepEqual(reflect.Zero(t).Interface(), v) {
			return true
		}
	}

	return false

}
