package onedb

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

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
}

func TestGetStruct(t *testing.T) {
	// success
	result := []SimpleData{}
	rows := newMockRowsScanner([]SimpleData{SimpleData{1, "hello"}, SimpleData{2, "world"}})
	err := GetStruct(rows, &result)
	if err != nil || len(result) != 2 || result[0].IntVal != 1 || result[0].StringVal != "hello" || result[1].IntVal != 2 || result[1].StringVal != "world" {
		t.Error("expected valid result", err, result)
	}

	// scan error
	result = []SimpleData{}
	rows = newMockRowsScanner([]SimpleData{SimpleData{1, "hello"}, SimpleData{2, "world"}})
	rows.ScanErr = errors.New("fail")
	err = GetStruct(rows, &result)
	if err == nil {
		t.Error("expected error")
	}

	// err error
	result = []SimpleData{}
	rows = &mockRowsScanner{ErrErr: errors.New("fail")}
	err = GetStruct(rows, &result)
	if err == nil {
		t.Error("expected error")
	}
}

func TestGetStructRow(t *testing.T) {
	// success
	result := SimpleData{}
	rows := newMockRowsScanner([]SimpleData{SimpleData{1, "hello"}})
	err := GetStructRow(rows, &result)
	if err != nil || result.IntVal != 1 || result.StringVal != "hello" {
		t.Error("expected valid result", err, result)
	}

	// scan error
	result = SimpleData{}
	rows = newMockRowsScanner([]SimpleData{SimpleData{1, "hello"}})
	rows.ScanErr = errors.New("fail")
	err = GetStructRow(rows, &result)
	if err == nil {
		t.Error("expected error")
	}

	// err error
	result = SimpleData{}
	rows = &mockRowsScanner{ErrErr: errors.New("fail")}
	err = GetStructRow(rows, &result)
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
	setValueRunner("BoolVal", true, t)
	setValueRunner("ByteVal", []byte("byte"), t)
	setValueRunner("Float32", float32(123.4567899), t)
	setValueRunner("Float64", float64(123.4567899), t)
	setValueRunner("Int", int(123), t)
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
	setValueRunner("StrPtr", "hello", t)
	setValueRunner("Int16Ptr", int16(123), t)
	setValueRunner("BoolPtr", true, t)
	setValueRunner("TimePtr", time.Date(2000, 01, 01, 12, 0, 0, 0, time.Local), t)
}

func TestSetValueEdgeCases(t *testing.T) {
	test := &TestStruct{}
	f := reflect.ValueOf(test).Elem().FieldByName("Time")
	v := new(interface{})
	*v = "hello"
	setValue(f, v)
	r := time.Time{}
	if test.Time != r {
		t.Error("expected unitialized time struct since we can't set it to a string")
	}

	f = reflect.ValueOf(test).Elem().FieldByName("notsettable")
	*v = 123
	setValue(f, v)
	if test.notsettable != 0 {
		t.Error("expected to be unable to set value")
	}
}

func setValueRunner(fieldName string, value interface{}, t *testing.T) {
	test := &TestStruct{}
	field := reflect.ValueOf(test).Elem().FieldByName(fieldName)
	iface := new(interface{})
	*iface = value
	setValue(field, iface)
	if field.Kind() == reflect.Slice {
		v := fmt.Sprintf("%v", value)
		if fv := fmt.Sprintf("%v", field.Interface()); fv != v {
			t.Error("expected slices to match", fv, v)
		}
	} else if field.Kind() == reflect.Ptr {
		if field.Elem().Interface() != value {
			t.Errorf("expected %s to be set to %v. Actual: %s", fieldName, value, field.Elem().Interface())
		}
	} else {
		if field.Interface() != value {
			t.Errorf("expected %s to be set to %v. Actual: %s", fieldName, value, field.Interface())
		}
	}
}

func TestGetItemTypeAndMap(t *testing.T) {
	item := TestItem{}
	itemType, dbToStructMap := getItemTypeAndMap([]string{"Str", "Nil", "Another"}, reflect.TypeOf(&item))
	if itemType != reflect.TypeOf(item) || dbToStructMap[0].Name != "str" || dbToStructMap[1].Name != "nil" {
		t.Error("expected different type and field map", itemType, dbToStructMap)
	}
}
