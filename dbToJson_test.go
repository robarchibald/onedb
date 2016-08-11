package onedb

import (
	"database/sql"
	"errors"
	"testing"
	"time"
)

func TestGetJsonWithSqlRows(t *testing.T) {
	rows := &sql.Rows{}
	_, err := getJson(rows)
	if err == nil {
		t.Fatal("expected failure with empty sql row")
	}
}

// should complete in less than .05s
func TestGetJsonWith10000SqlRows(t *testing.T) {
	rows := &MockRows{NumRows: 10000}
	getJson(rows)
}

func TestGetJsonWithFakeRows(t *testing.T) {
	rows := &MockRows{NumRows: 2}
	json, _ := getJson(rows)
	if json != `[{"str":"string\n\twith carriage return","int":1,"date":"2000-01-01 12:00:00","true":true,"false":false,"byte":"Ynl0ZQ=="},{"str":"string\n\twith carriage return","int":1,"date":"2000-01-01 12:00:00","true":true,"false":false,"byte":"Ynl0ZQ=="}]` {
		t.Fatal("expected matching json", json)
	}
}

func TestGetJsonWithErroringRows(t *testing.T) {
	rows := &MockErroringRows{RowsErr: errors.New("failed")}
	_, err := getJson(rows)
	if err == nil {
		t.Fatal("expected failure with error")
	}
}

func TestGetJsonWithScanError(t *testing.T) {
	rows := &MockErroringRows{ScanErr: errors.New("failed")}
	_, err := getJson(rows)
	if err == nil {
		t.Fatal("expected failure with error")
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
func (m *MockRows) Close() error {
	return nil
}
func (m *MockRows) Scan(dest ...interface{}) error {
	var nilVal interface{} = nil
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

type MockErroringRows struct {
	RowsErr error
	ScanErr error
}

func (m *MockErroringRows) Columns() ([]string, error) {
	return []string{"str"}, nil
}
func (m *MockErroringRows) Next() bool {
	return true
}
func (m *MockErroringRows) Close() error {
	return nil
}
func (m *MockErroringRows) Scan(dest ...interface{}) error {
	return m.ScanErr
}

func (m *MockErroringRows) Err() error {
	return m.RowsErr
}
