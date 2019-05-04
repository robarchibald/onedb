package onedb

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func TestBackendConverterQueryJson(t *testing.T) {
	rows := newMockRowsScanner([]SimpleData{SimpleData{1, "hello"}})
	db := &mockBackend{Rows: rows}

	json, err := QueryJSON(db, "select * from TestTable")
	if json != "[{\"IntVal\":1,\"StringVal\":\"hello\"}]" {
		t.Error("expected different json back.  Actual:", json, err)
	}
	if err != nil {
		t.Error("didn't expect error")
	}

	db = &mockBackend{QueryErr: errors.New("fail")}
	_, err = QueryJSON(db, "select * from TestTable")
	if err == nil {
		t.Error("expected error")
	}
}

func TestBackendConverterQueryJsonRow(t *testing.T) {
	rows := newMockRowsScanner([]SimpleData{SimpleData{1, "hello"}})
	db := &mockBackend{Rows: rows}

	json, err := QueryJSONRow(db, "select * from TestTable")
	if json != "{\"IntVal\":1,\"StringVal\":\"hello\"}" {
		t.Error("expected different json back.  Actual:", json, err)
	}
	if err != nil {
		t.Error("didn't expect error")
	}

	db = &mockBackend{QueryErr: errors.New("fail")}
	_, err = QueryJSONRow(db, "select * from TestTable")
	if err == nil {
		t.Error("expected error")
	}
}

func TestBackendConverterQueryStruct(t *testing.T) {
	rows := newMockRowsScanner([]SimpleData{SimpleData{1, "hello"}})
	db := &mockBackend{Rows: rows}
	data := []SimpleData{}

	// wrong receiver type
	err := QueryStruct(db, data, "query")
	if err == nil {
		t.Error("expected error", err)
	}

	// success
	err = QueryStruct(db, &data, "query")
	if err != nil || len(data) != 1 || data[0].IntVal != 1 || data[0].StringVal != "hello" {
		t.Error("expected success")
	}

	// query error
	db = &mockBackend{QueryErr: errors.New("fail")}
	err = QueryStruct(db, &data, "query")
	if err == nil {
		t.Error("expected error", err)
	}
}

func TestBackendConverterQueryStructRow(t *testing.T) {
	rows := newMockRowsScanner([]SimpleData{SimpleData{1, "hello"}})
	db := &mockBackend{Rows: rows}
	data := SimpleData{}

	// wrong receiver type
	err := QueryStructRow(db, data, "query")
	if err == nil {
		t.Error("expected error", err)
	}

	// success
	err = QueryStructRow(db, &data, "query")
	if err != nil || data.IntVal != 1 || data.StringVal != "hello" {
		t.Error("expected success")
	}

	// query error
	db = &mockBackend{QueryErr: errors.New("fail")}
	err = QueryStructRow(db, &data, "query")
	if err == nil {
		t.Error("expected error", err)
	}
}

/******************** MOCKS ************************/
type mockBackend struct {
	Rows     RowsScanner
	Row      Scanner
	ExecErr  error
	QueryErr error
}

func (b *mockBackend) Query(query string, args ...interface{}) (RowsScanner, error) {
	return b.Rows, b.QueryErr
}
func (b *mockBackend) QueryRow(query string, args ...interface{}) Scanner {
	if b.QueryErr != nil {
		return &mockScanner{ScanErr: b.QueryErr}
	}
	return b.Row
}

type mockRowsScanner struct {
	sliceValue reflect.Value
	sliceLen   int
	structType reflect.Type
	structLen  int
	data       interface{}
	currentRow int
	ColumnsErr error
	ScanErr    error
	ErrErr     error
}

func newMockRowsScanner(data interface{}) *mockRowsScanner {
	if data == nil || reflect.TypeOf(data).Kind() != reflect.Slice || reflect.TypeOf(data).Elem().Kind() != reflect.Struct {
		return &mockRowsScanner{ScanErr: ErrRowsScannerInvalidData, ErrErr: ErrRowsScannerInvalidData}
	}
	sliceValue := reflect.ValueOf(data)
	sliceLen := sliceValue.Len()
	structType := reflect.TypeOf(data).Elem()
	structLen := structType.NumField()

	return &mockRowsScanner{data: data, currentRow: -1, sliceValue: sliceValue, sliceLen: sliceLen, structType: structType, structLen: structLen}
}

func (r *mockRowsScanner) Columns() ([]string, error) {
	if r.ColumnsErr != nil {
		return nil, r.ColumnsErr
	}

	columns := make([]string, r.structLen)
	for i := 0; i < r.structLen; i++ {
		columns[i] = r.structType.Field(i).Name
	}
	return columns, nil
}

func (r *mockRowsScanner) Next() bool {
	r.currentRow++
	if r.currentRow >= r.sliceLen {
		return false
	}
	return true
}
func (r *mockRowsScanner) Close() {}
func (r *mockRowsScanner) Scan(dest ...interface{}) error {
	if r.ScanErr != nil {
		return r.ScanErr
	}
	if r.currentRow >= r.sliceLen || r.currentRow < 0 {
		return errors.New("invalid current row")
	}
	return setDestValue(r.sliceValue.Index(r.currentRow), dest)
}

func setDestValue(structVal reflect.Value, dest []interface{}) error {
	if len(dest) != structVal.NumField() {
		return fmt.Errorf("expected equal number of dest values as source. Expected: %d, Actual: %d", structVal.NumField(), len(dest))
	}
	for i := range dest {
		destination := reflect.ValueOf(dest[i]).Elem()
		source := structVal.Field(i)
		if destination.Type() != source.Type() && destination.Type().Kind() != reflect.Interface {
			return fmt.Errorf("source and destination types do not match at index: %d", i)
		}
		destination.Set(source)
	}
	return nil
}

func (r *mockRowsScanner) Err() error {
	return r.ErrErr
}

type mockScanner struct {
	structValue reflect.Value
	data        interface{}
	ScanErr     error
}

func newMockScanner(data interface{}) *mockScanner {
	if data == nil || reflect.TypeOf(data).Kind() != reflect.Ptr || reflect.TypeOf(data).Elem().Kind() != reflect.Struct {
		return &mockScanner{ScanErr: ErrRowScannerInvalidData}
	}
	structValue := reflect.ValueOf(data).Elem()
	return &mockScanner{data: data, structValue: structValue}
}

func (s *mockScanner) Scan(dest ...interface{}) error {
	if s.ScanErr != nil {
		return s.ScanErr
	}
	return setDestValue(s.structValue, dest)
}
