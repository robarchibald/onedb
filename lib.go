package onedb

import (
	"errors"
	"fmt"
	"net"
	"reflect"
	"time"
)

// this file contains functions, types and methods from the pgxo library that are required by both the main and pgx libraries

// main items

type Dialer interface {
	Dial(network, addr string) (net.Conn, error)
}

func IsPointer(item reflect.Type) bool {
	return item.Kind() == reflect.Ptr
}

func IsSlice(item reflect.Type) bool {
	return item.Kind() == reflect.Slice
}

var DialHelper Dialer = &RealDialer{}

type RealDialer struct{}

func (d *RealDialer) Dial(network, addr string) (net.Conn, error) {
	tcpAddr, err := net.ResolveTCPAddr(network, addr)
	if err != nil {
		return nil, err
	}
	tc, err := net.DialTCP(network, nil, tcpAddr)
	if err != nil {
		return nil, err
	}
	if err := tc.SetKeepAlive(true); err != nil {
		return nil, err
	}
	if err := tc.SetKeepAlivePeriod(2 * time.Minute); err != nil {
		return nil, err
	}
	return tc, nil
}

type ErrorScanner struct {
	Err error
}

func (s *ErrorScanner) Scan(dest ...interface{}) error {
	return s.Err
}

//test items

func (d *MockDialer) Dial(network, addr string) (net.Conn, error) {
	return nil, d.Err
}

type MockDialer struct {
	Err error
}

type mockRowsScanner struct {
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
func (r *mockRowsScanner) Close() error {
	return r.CloseErr
}
func (r *mockRowsScanner) Scan(dest ...interface{}) error {
	if r.ScanErr != nil {
		return r.ScanErr
	}
	if r.currentRow >= r.sliceLen || r.currentRow < 0 {
		return errors.New("invalid current row")
	}
	return setDestValue(r.sliceValue.Index(r.currentRow), dest)
}

func (r *mockRowsScanner) Err() error {
	return r.ErrErr
}

func NewMockRowsScanner(data interface{}) *mockRowsScanner {
	if data == nil || reflect.TypeOf(data).Kind() != reflect.Slice || reflect.TypeOf(data).Elem().Kind() != reflect.Struct {
		return &mockRowsScanner{ScanErr: ErrRowsScannerInvalidData, ErrErr: ErrRowsScannerInvalidData}
	}
	sliceValue := reflect.ValueOf(data)
	sliceLen := sliceValue.Len()
	structType := reflect.TypeOf(data).Elem()
	structLen := structType.NumField()

	return &mockRowsScanner{data: data, currentRow: -1, sliceValue: sliceValue, sliceLen: sliceLen, structType: structType, structLen: structLen}
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

var ErrRowsScannerInvalidData = errors.New("data must be a slice of structs")
var ErrRowScannerInvalidData = errors.New("data must be a ptr to a struct")

type SimpleData struct {
	IntVal    int
	StringVal string
}

//mocks
type mockBackend struct {
	Rows     rowsScanner
	Row      scanner
	CloseErr error
	ExecErr  error
	QueryErr error
}

func (b *mockBackend) Close() error {
	return b.CloseErr
}
func (b *mockBackend) Execute(query interface{}) error {
	return b.ExecErr
}
func (b *mockBackend) Query(query interface{}) (rowsScanner, error) {
	return b.Rows, b.QueryErr
}
func (b *mockBackend) QueryRow(query interface{}) scanner {
	if b.QueryErr != nil {
		return &MockScanner{ScanErr: b.QueryErr}
	}
	return b.Row
}

//
type MockScanner struct {
	structValue reflect.Value
	data        interface{}
	ScanErr     error
}

func newMockScanner(data interface{}) *MockScanner {
	if data == nil || reflect.TypeOf(data).Kind() != reflect.Ptr || reflect.TypeOf(data).Elem().Kind() != reflect.Struct {
		return &MockScanner{ScanErr: ErrRowScannerInvalidData}
	}
	structValue := reflect.ValueOf(data).Elem()
	return &MockScanner{data: data, structValue: structValue}
}

func (s *MockScanner) Scan(dest ...interface{}) error {
	if s.ScanErr != nil {
		return s.ScanErr
	}
	return setDestValue(s.structValue, dest)
}
