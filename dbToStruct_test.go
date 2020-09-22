package onedb

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

type SimpleItem struct {
	Str string
}

type TestItem struct {
	Str   string
	Date  time.Time
	False bool
	Nil   string
	True  bool
	Int   int
	Byte  []byte
}

type TestStruct struct {
	BoolVal     bool
	ByteVal     []byte
	Float32     float32
	Float64     float64
	Int         int
	Int8        int8
	Int16       int16
	Int32       int32
	Int64       int64
	Uint8       uint8
	Uint16      uint16
	Uint32      uint32
	Uint64      uint64
	String      string
	StrSlice    []string
	StrPtr      *string
	BoolPtr     *bool
	Int16Ptr    *int16
	TimePtr     *time.Time
	Time        time.Time
	notsettable int
	StructVal   SimpleItem
	StructPtr   *SimpleItem
}

func TestGetStruct(t *testing.T) {
	// success
	result := []SimpleData{}
	rows := NewRowsScanner([]SimpleData{{1, "hello"}, {2, "world"}})
	err := getStruct(rows, &result)
	if err != nil || len(result) != 2 || result[0].IntVal != 1 || result[0].StringVal != "hello" || result[1].IntVal != 2 || result[1].StringVal != "world" {
		t.Error("expected valid result", err, result)
	}

	// scan error
	result = []SimpleData{}
	rows = NewRowsScanner([]SimpleData{{1, "hello"}, {2, "world"}})
	rows.(*mockRowsScanner).ScanErr = errors.New("fail")
	err = getStruct(rows, &result)
	if err == nil {
		t.Error("expected error")
	}

	// err error
	result = []SimpleData{}
	rows = NewRowsScanner(nil)
	err = getStruct(rows, &result)
	if err == nil {
		t.Error("expected error")
	}
}

func TestGetStructRow(t *testing.T) {
	// success
	result := SimpleData{}
	rows := NewRowsScanner([]SimpleData{{1, "hello"}})
	err := getStructRow(rows, &result)
	if err != nil || result.IntVal != 1 || result.StringVal != "hello" {
		t.Error("expected valid result", err, result)
	}

	// scan error
	result = SimpleData{}
	rows = NewRowsScanner([]SimpleData{{1, "hello"}})
	rows.(*mockRowsScanner).ScanErr = errors.New("fail")
	err = getStructRow(rows, &result)
	if err == nil {
		t.Error("expected error")
	}

	// err error
	result = SimpleData{}
	rows = NewRowsScanner(nil)
	err = getStructRow(rows, &result)
	if err == nil {
		t.Error("expected error")
	}
}

func TestStructRow(t *testing.T) {
	item := TestItem{}
	rows := &MockRows{NumRows: 1}
	expStr := `string
	with carriage return`
	var col1, col2 string
	var col3 int
	var col4 time.Time
	var col5, col6 bool
	var col7 []byte
	vals := []interface{}{&col1, &col2, &col3, &col4, &col5, &col6, &col7}
	_, dbToStructMap := getItemTypeAndMap([]string{"Nil", "Str", "Int", "Date", "True", "False", "Byte"}, reflect.TypeOf(&item))
	scanStruct(rows, vals, dbToStructMap, &item)
	if item.Str != expStr || item.Nil != "" || item.Int != 1 || item.True != true {
		t.Error("expected to contain values", item)
	}
}

func TestSetValue(t *testing.T) {
	v := "hello"
	i := 0
	setValueRunner("BoolVal", true, t)
	setValueRunner("ByteVal", []byte("byte"), t)
	setValueRunner("Float32", float32(123.4567899), t)
	setValueRunner("Float32", float64(123.4567899), t)
	setValueRunner("Float64", float32(123.4567899), t)
	setValueRunner("Float64", float64(123.4567899), t)
	setValueRunner("Int", int(123), t)
	setValueExpectEmpty("Int", &i, t) // don't allow setting nullable value to non nullable destination
	setValueRunner("Int", int8(123), t)
	setValueExpectEmpty("Int", "hello", t)
	setValueRunner("Int8", int8(123), t)
	setValueRunner("Int16", int16(123), t)
	setValueRunner("Int32", int32(123), t)
	setValueRunner("Int64", int64(123), t)
	setValueRunner("Uint8", uint8(123), t)
	setValueRunner("Uint16", uint16(123), t)
	setValueRunner("Uint32", uint32(123), t)
	setValueRunner("Uint64", uint64(123), t)
	setValueRunner("String", "hello", t)
	setValueRunner("Time", time.Date(2000, 01, 01, 12, 0, 0, 0, time.Local), t)
	setValueRunner("StrSlice", []string{"hello", "there"}, t)
	setValueRunner("StructVal", SimpleItem{Str: "str"}, t)
	setValueRunner("StructPtr", &SimpleItem{Str: "str"}, t)
	setValueRunner("StrPtr", "hello", t)
	setValueRunner("StrPtr", &v, t)
	setValueExpectEmpty("StrPtr", &i, t)
	setValueRunner("Int16Ptr", int16(123), t)
	setValueExpectEmpty("Int16Ptr", "hello", t)
	setValueRunner("BoolPtr", true, t)
	setValueRunner("TimePtr", time.Date(2000, 01, 01, 12, 0, 0, 0, time.Local), t)
}

func TestSetValueEdgeCases(t *testing.T) {
	test := &TestStruct{}
	f := reflect.ValueOf(test).Elem().FieldByName("Time")
	v := new(interface{})
	*v = "hello"
	SetValue(f, v)
	r := time.Time{}
	if test.Time != r {
		t.Error("expected unitialized time struct since we can't set it to a string")
	}

	f = reflect.ValueOf(test).Elem().FieldByName("notsettable")
	*v = 123
	SetValue(f, v)
	if test.notsettable != 0 {
		t.Error("expected to be unable to set value")
	}
}

func setValueRunner(fieldName string, value interface{}, t *testing.T) {
	test := &TestStruct{}
	dest := reflect.ValueOf(test).Elem().FieldByName(fieldName)
	iface := new(interface{})
	*iface = value
	SetValue(dest, iface)
	if dest.Kind() == reflect.Slice {
		v := fmt.Sprintf("%v", value)
		if fv := fmt.Sprintf("%v", dest.Interface()); fv != v {
			t.Error("expected slices to match", fv, v)
		}
	} else if dest.Kind() == reflect.Ptr {
		v := reflect.ValueOf(value)
		if v.Kind() == reflect.Ptr {
			if dest.Elem() == reflect.ValueOf(nil) {
				t.Errorf("expected %s to be set to %v. Actual: nil", fieldName, v.Elem().Interface())
			} else if dest.Elem().Interface() != v.Elem().Interface() {
				t.Errorf("expected %s to be set to %v. Actual: %s", fieldName, v.Elem().Interface(), dest.Elem().Interface())
			}
		} else {
			if dest.Elem() == reflect.ValueOf(nil) {
				t.Errorf("expected %s to be set to %v. Actual: nil", fieldName, value)
			} else if dest.Elem().Interface() != value {
				t.Errorf("expected %s to be set to %v. Actual: %s", fieldName, value, dest.Elem().Interface())
			}
		}
	} else {
		if fmt.Sprintf("%v", dest.Interface()) != fmt.Sprintf("%v", value) {
			t.Errorf("expected %s to be set to %v. Actual: %v", fieldName, value, dest.Interface())
		}
	}
}

func setValueExpectEmpty(fieldName string, value interface{}, t *testing.T) {
	test := &TestStruct{}
	dest := reflect.ValueOf(test).Elem().FieldByName(fieldName)
	iface := new(interface{})
	*iface = value
	SetValue(dest, iface)

	zeroValue := reflect.New(dest.Type()).Elem().Interface()
	if dest.Interface() != zeroValue {
		t.Errorf("expected %s to be empty. Expected: %v, Actual: %v", fieldName, zeroValue, dest.Interface())
	}
}

func TestGetItemTypeAndMap(t *testing.T) {
	item := TestItem{}
	itemType, dbToStructMap := getItemTypeAndMap([]string{"Str", "Nil", "Another"}, reflect.TypeOf(&item))
	if itemType != reflect.TypeOf(item) || dbToStructMap[0].Name != "str" || dbToStructMap[1].Name != "nil" {
		t.Error("expected different type and field map", itemType, dbToStructMap)
	}
}
