package onedb

import (
	"errors"
	"testing"
	"time"
	"unicode"
)

func TestGetJson(t *testing.T) {
	// success
	rows := newMockRowsScanner([]SimpleData{SimpleData{1, "hello"}, SimpleData{2, "world"}})
	json, _ := GetJSON(rows)
	if json != `[{"IntVal":1,"StringVal":"hello"},{"IntVal":2,"StringVal":"world"}]` {
		t.Error("expected valid json", json)
	}

	// scan error
	rows = newMockRowsScanner([]SimpleData{SimpleData{1, "hello"}, SimpleData{2, "world"}})
	rows.ScanErr = errors.New("fail")
	_, err := GetJSON(rows)
	if err == nil {
		t.Error("expected error")
	}

	// err error
	rows = &mockRowsScanner{ErrErr: errors.New("fail")}
	_, err = GetJSON(rows)
	if err == nil {
		t.Error("expected error")
	}
}

func TestGetJsonRow(t *testing.T) {
	// success
	rows := newMockRowsScanner([]SimpleData{SimpleData{1, "hello"}})
	json, _ := GetJSONRow(rows)
	if json != `{"IntVal":1,"StringVal":"hello"}` {
		t.Error("expected valid json", json)
	}

	// scan error
	rows = newMockRowsScanner([]SimpleData{SimpleData{1, "hello"}})
	rows.ScanErr = errors.New("fail")
	_, err := GetJSONRow(rows)
	if err == nil {
		t.Error("expected error")
	}

	// err error
	rows = &mockRowsScanner{ErrErr: errors.New("fail")}
	_, err = GetJSONRow(rows)
	if err == nil {
		t.Error("expected error")
	}
}

func TestGetColumnNamesAndValues(t *testing.T) {
	// row err
	rows := &mockRowsScanner{ErrErr: errors.New("fail")}
	_, _, err := getColumnNamesAndValues(rows, true)
	if err == nil {
		t.Error("expected failure")
	}

	// columns err
	rows = &mockRowsScanner{ColumnsErr: errors.New("fail")}
	_, _, err = getColumnNamesAndValues(rows, true)
	if err == nil {
		t.Error("expected failure")
	}

	// json columns
	rows = newMockRowsScanner([]SimpleData{SimpleData{1, "hello"}})
	cols, vals, _ := getColumnNamesAndValues(rows, true)
	if len(cols) != 2 || cols[0] != `"IntVal":` || cols[1] != `"StringVal":` || len(vals) != 2 {
		t.Error("expected valid column names and values array", cols, vals)
	}

	// non-json columns
	rows = newMockRowsScanner([]SimpleData{SimpleData{1, "hello"}})
	cols, vals, _ = getColumnNamesAndValues(rows, false)
	if len(cols) != 2 || cols[0] != "IntVal" || cols[1] != "StringVal" || len(vals) != 2 {
		t.Error("expected valid column names and values array", cols, vals)
	}
}

func TestGetJsonValue(t *testing.T) {
	p := new(interface{})
	if actual := getJSONValue(p); actual != "null" {
		t.Error("expected value: null", actual)
	}

	*p = true
	if actual := getJSONValue(p); actual != "true" {
		t.Error("expected value: true", actual)
	}

	*p = false
	if actual := getJSONValue(p); actual != "false" {
		t.Error("expected value: false", actual)
	}

	*p = []byte("byte") // "byte" base64 encoded: Ynl0ZQ==
	if actual := getJSONValue(p); actual != `"Ynl0ZQ=="` {
		t.Error("expected value: \"Ynl0ZQ==\"", actual)
	}

	*p = time.Date(2000, 1, 2, 3, 4, 5, 123456789, time.UTC)
	if actual := getJSONValue(p); actual != `"2000-01-02 03:04:05.123"` {
		t.Error("expected value: \"2000-01-02 03:04:05.123\"", actual)
	}

	*p = 12
	if actual := getJSONValue(p); actual != "12" {
		t.Error("expected value: 12", actual)
	}

	*p = "hello"
	if actual := getJSONValue(p); actual != `"hello"` {
		t.Error("expected value \"hello\"", actual)
	}

	*p = []string{"hello", "world"}
	if actual := getJSONValue(p); actual != `"[hello world]"` {
		t.Error("expected value \"[hello world]\"", actual)
	}
}

func TestEncodeByteSlice(t *testing.T) {
	if actual := encodeByteSlice([]byte{}); actual != "null" {
		t.Error("expected value: null", actual)
	}

	if actual := encodeByteSlice([]byte("byte")); actual != `"Ynl0ZQ=="` {
		t.Error("expected value: \"Ynl0ZQ==\"", actual)
	}

	b := []byte("really long byte slice. Must be > 1024 for this to trigger...................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................")
	if actual := encodeByteSlice(b); actual != `"cmVhbGx5IGxvbmcgYnl0ZSBzbGljZS4gTXVzdCBiZSA+IDEwMjQgZm9yIHRoaXMgdG8gdHJpZ2dlci4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLi4uLg=="` {
		t.Error("expected value differs", actual)
	}
}

// taken from encoding/json encode_test.go. Useless test, but it hits every part of the code
func TestEncodeString(t *testing.T) {
	var r []rune
	for i := '\u0000'; i <= unicode.MaxRune; i++ {
		r = append(r, i)
	}
	s := string(r) + "\xff\xff\xffhello" // some invalid UTF-8 too
	encodeString(s)
}

// should complete in less than .05s
func TestGetJsonWith10000SqlRows(t *testing.T) {
	rows := &MockRows{NumRows: 10000}
	GetJSON(rows)
}

func TestGetJsonWithFakeRows(t *testing.T) {
	rows := &MockRows{NumRows: 2}
	json, _ := GetJSON(rows)
	if json != `[{"str":"string\n\twith carriage return","int":1,"date":"2000-01-01 12:00:00","true":true,"false":false,"byte":"Ynl0ZQ=="},{"str":"string\n\twith carriage return","int":1,"date":"2000-01-01 12:00:00","true":true,"false":false,"byte":"Ynl0ZQ=="}]` {
		t.Fatal("expected matching json", json)
	}
}

/******************************* Mocks ***************************************/
type TestData struct {
	Nil   interface{}
	Str   string
	Int   int
	Date  time.Time
	True  bool
	False bool
	Byte  []byte
}

type MockRows struct {
	NumRows int
}

func (m *MockRows) Columns() ([]string, error) {
	return []string{"nil", "str", "int", "date", "true", "false", "byte"}, nil
}
func (m *MockRows) Next() bool {
	m.NumRows--
	if m.NumRows < 0 {
		return false
	}
	return true
}
func (m *MockRows) Close() {}
func (m *MockRows) Scan(dest ...interface{}) error {
	var nilVal interface{}
	var strVal interface{} = `string
	with carriage return`
	var intVal interface{} = 1
	var dateVal interface{} = time.Date(2000, 01, 01, 12, 0, 0, 0, time.Local)
	var trueVal interface{} = true
	var falseVal interface{} = false
	var byteVal interface{} = []byte("byte") // will base64 encode to Ynl0ZQ==
	dest[0] = &nilVal
	dest[1] = &strVal
	dest[2] = &intVal
	dest[3] = &dateVal
	dest[4] = &trueVal
	dest[5] = &falseVal
	dest[6] = &byteVal
	return nil
}

func (m *MockRows) Err() error {
	return nil
}
