package onedb

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func TestSimpleQueryJson(t *testing.T) {
	rows := NewMockRowsScanner([]SimpleData{SimpleData{1, "hello"}})
	db := NewBackendConverter(&MockBackend{Rows: rows})

	json, err := db.QueryJson("select * from TestTable")
	if json != "[{\"IntVal\":1,\"StringVal\":\"hello\"}]" {
		t.Fatal("expected different json back.  Actual:", json, err)
	}
	if err != nil {
		t.Fatal("didn't expect error")
	}
}

func TestQueryStructNotPointerToSlice(t *testing.T) {
	conn := &BackendConverter{}
	err := conn.QueryStruct("no query", []TestItem{})
	if err == nil {
		t.Fatal("expected error", err)
	}
}

func TestQueryStructRowNotPointer(t *testing.T) {
	conn := &BackendConverter{}
	err := conn.QueryStructRow("no query", TestItem{})
	if err == nil {
		t.Fatal("expected error", err)
	}
}

/******************** MOCKS ************************/
var ErrRowsScannerInvalidData = errors.New("data must be a slice of structs")
var ErrRowScannerInvalidData = errors.New("data must be a ptr to a struct")

type MockBackend struct {
	Rows     RowsScanner
	Row      RowScanner
	CloseErr error
	ExecErr  error
	QueryErr error
}

func (b *MockBackend) Close() error {
	return b.CloseErr
}
func (b *MockBackend) Execute(query interface{}) error {
	return b.ExecErr
}
func (b *MockBackend) Query(query interface{}) (RowsScanner, error) {
	return b.Rows, b.QueryErr
}
func (b *MockBackend) QueryRow(query interface{}) RowScanner {
	if b.QueryErr != nil {
		return &MockRowScanner{ScanErr: b.QueryErr}
	}
	return b.Row
}

type MockRowsScanner struct {
	sliceValue reflect.Value
	sliceLen   int
	structType reflect.Type
	structLen  int
	data       interface{}
	currentRow int
	ColumnsErr error
	CloseErr   error
	ScanErr    error
	ErrErr     error
}

func NewMockRowsScanner(data interface{}) *MockRowsScanner {
	if data == nil || reflect.TypeOf(data).Kind() != reflect.Slice || reflect.TypeOf(data).Elem().Kind() != reflect.Struct {
		return &MockRowsScanner{ScanErr: ErrRowsScannerInvalidData, ErrErr: ErrRowsScannerInvalidData}
	}
	sliceValue := reflect.ValueOf(data)
	sliceLen := sliceValue.Len()
	structType := reflect.TypeOf(data).Elem()
	structLen := structType.NumField()

	return &MockRowsScanner{data: data, currentRow: -1, sliceValue: sliceValue, sliceLen: sliceLen, structType: structType, structLen: structLen}
}

func (r *MockRowsScanner) Columns() ([]string, error) {
	if r.ColumnsErr != nil {
		return nil, r.ColumnsErr
	}

	columns := make([]string, r.structLen)
	for i := 0; i < r.structLen; i++ {
		columns[i] = r.structType.Field(i).Name
	}
	return columns, nil
}

func (r *MockRowsScanner) Next() bool {
	r.currentRow++
	if r.currentRow >= r.sliceLen {
		return false
	}
	return true
}
func (r *MockRowsScanner) Close() error {
	return r.CloseErr
}
func (r *MockRowsScanner) Scan(dest ...interface{}) error {
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
	for i, _ := range dest {
		destination := reflect.ValueOf(dest[i]).Elem()
		source := structVal.Field(i)
		if destination.Type() != source.Type() && destination.Type().Kind() != reflect.Interface {
			return fmt.Errorf("source and destination types do not match at index: %d", i)
		}
		destination.Set(source)
	}
	return nil
}

func (r *MockRowsScanner) Err() error {
	return r.ErrErr
}

type MockRowScanner struct {
	structValue reflect.Value
	data        interface{}
	ScanErr     error
}

func NewMockRowScanner(data interface{}) *MockRowScanner {
	if data == nil || reflect.TypeOf(data).Kind() != reflect.Ptr || reflect.TypeOf(data).Elem().Kind() != reflect.Struct {
		return &MockRowScanner{ScanErr: ErrRowScannerInvalidData}
	}
	structValue := reflect.ValueOf(data).Elem()
	return &MockRowScanner{data: data, structValue: structValue}
}

func (s *MockRowScanner) Scan(dest ...interface{}) error {
	if s.ScanErr != nil {
		return s.ScanErr
	}
	return setDestValue(s.structValue, dest)
}
