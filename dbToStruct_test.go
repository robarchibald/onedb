package onedb

import (
	"errors"
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
	BVal        bool
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
	Ptr         *string
	String      string
	Time        time.Time
	notsettable int
}

func TestGetStruct(t *testing.T) {
	items := []TestStruct{}
	rows := &MockRows{NumRows: 20}
	getStruct(rows, &items)
	if len(items) != 20 {
		t.Fatal("expected to contain 20 rows", items)
	}
}

func TestGetStructScanErr(t *testing.T) {
	items := []TestStruct{}
	rows := &MockErroringRows{ScanErr: errors.New("error")}
	err := getStruct(rows, &items)
	if err == nil {
		t.Fatal("expected error", err)
	}
}

func TestGetStructRowErr(t *testing.T) {
	items := []TestStruct{}
	rows := &MockErroringRows{RowsErr: errors.New("error")}
	err := getStruct(rows, &items)
	if err == nil {
		t.Fatal("expected error", err)
	}
}

func TestGetStructRow(t *testing.T) {
	items := TestItem{}
	rows := &MockRows{NumRows: 1}
	expStr := `string
	with carriage return`
	getStructRow(rows, &items)
	if items.Str != expStr || items.Nil != "" || items.Int != 1 || items.True != true {
		t.Fatal("expected to contain values", items)
	}
}

func TestGetStructRowScanErr(t *testing.T) {
	item := TestItem{}
	rows := &MockErroringRows{ScanErr: errors.New("error")}
	err := getStructRow(rows, &item)
	if err == nil {
		t.Fatal("expected error", err)
	}
}

func TestGetStructRowRowErr(t *testing.T) {
	item := TestItem{}
	rows := &MockErroringRows{RowsErr: errors.New("error")}
	err := getStructRow(rows, &item)
	if err == nil {
		t.Fatal("expected error", err)
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
	structRow(rows, vals, dbToStructMap, &item)
	if item.Str != expStr || item.Nil != "" || item.Int != 1 || item.True != true {
		t.Fatal("expected to contain values", item)
	}
}

func TestSetValuePointer(t *testing.T) {
	test := &TestStruct{}
	setValueRunner(test, "Ptr", "hello")
	if *test.Ptr != "hello" {
		t.Fatal("expected Ptr to be set", test.Ptr)
	}
}

func TestSetValueDate(t *testing.T) {
	test := &TestStruct{}
	setValueRunner(test, "Time", time.Date(2000, 01, 01, 12, 0, 0, 0, time.Local))
	actual := test.Time
	if actual.Year() != 2000 || actual.Month() != 1 || actual.Day() != 1 || actual.Hour() != 12 {
		t.Fatal("expected time to be set", actual)
	}
}

func TestSetValueFloat64(t *testing.T) {
	test := &TestStruct{}
	setValueRunner(test, "Float64", float64(123.4567899))
	if test.Float64 != 123.4567899 {
		t.Fatal("expected float64 to be set", test.Float64)
	}
}

func TestSetValueFloat32(t *testing.T) {
	test := &TestStruct{}
	setValueRunner(test, "Float32", float32(123.4567899))
	if test.Float32 != 123.4567899 {
		t.Fatal("expected float32 to be set", test.Float32)
	}
}

func TestSetValueInt8(t *testing.T) {
	test := &TestStruct{}
	setValueRunner(test, "Int8", int8(123))
	if test.Int8 != 123 {
		t.Fatal("expected int8 to be set", test.Int8)
	}
}

func TestSetValueInt16(t *testing.T) {
	test := &TestStruct{}
	setValueRunner(test, "Int16", int16(123))
	if test.Int16 != 123 {
		t.Fatal("expected int16 to be set", test.Int16)
	}
}

func TestSetValueInt32(t *testing.T) {
	test := &TestStruct{}
	setValueRunner(test, "Int32", int32(123))
	if test.Int32 != 123 {
		t.Fatal("expected int32 to be set", test.Int32)
	}
}

func TestSetValueInt64(t *testing.T) {
	test := &TestStruct{}
	setValueRunner(test, "Int64", int64(123))
	if test.Int64 != 123 {
		t.Fatal("expected int64 to be set", test.Int64)
	}
}

func TestSetValueUInt8(t *testing.T) {
	test := &TestStruct{}
	setValueRunner(test, "Uint8", uint8(123))
	if test.Uint8 != 123 {
		t.Fatal("expected uint8 to be set", test.Uint8)
	}
}

func TestSetValueUInt16(t *testing.T) {
	test := &TestStruct{}
	setValueRunner(test, "Uint16", uint16(123))
	if test.Uint16 != 123 {
		t.Fatal("expected uint16 to be set", test.Uint16)
	}
}

func TestSetValueUInt32(t *testing.T) {
	test := &TestStruct{}
	setValueRunner(test, "Uint32", uint32(123))
	if test.Uint32 != 123 {
		t.Fatal("expected uint32 to be set", test.Uint32)
	}
}

func TestSetValueUInt64(t *testing.T) {
	test := &TestStruct{}
	setValueRunner(test, "Uint64", uint64(123))
	if test.Uint64 != 123 {
		t.Fatal("expected uint64 to be set", test.Uint64)
	}
}

func TestSetValueCantSet(t *testing.T) {
	test := &TestStruct{}
	setValueRunner(test, "notsettable", int(123))
	if test.notsettable != 0 {
		t.Fatal("expected notsettable to not be settable", test.notsettable)
	}
}

func setValueRunner(test *TestStruct, fieldName string, value interface{}) {
	field := reflect.ValueOf(test).Elem().FieldByName(fieldName)
	iface := new(interface{})
	*iface = value
	setValue(field, iface)
}

func TestGetItemTypeAndMap(t *testing.T) {
	item := TestItem{}
	itemType, dbToStructMap := getItemTypeAndMap([]string{"Str", "Nil", "Another"}, reflect.TypeOf(&item))
	if itemType != reflect.TypeOf(item) || dbToStructMap[0].Name != "str" || dbToStructMap[1].Name != "nil" {
		t.Fatal("expected different type and field map", itemType, dbToStructMap)
	}
}
