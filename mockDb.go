package onedb

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"
)

type mockDb struct {
	data       []interface{}
	methodsRun []MethodsRun
	closeErr   error
	execErr    error
}

// MethodsRun contains the name of the method run and a slice of arguments
type MethodsRun struct {
	MethodName string
	Arguments  []interface{}
}

// Mocker is a fake database that can be used in place of a pgx or sql lib database for testing
type Mocker interface {
	DBer
	Query(query string, args ...interface{}) (RowsScanner, error)
	QueryRow(query string, args ...interface{}) Scanner
	QueriesRun() []MethodsRun
	SaveMethodCall(name string, arguments []interface{})
	VerifyNextCommand(t *testing.T, name string, expected ...interface{})
}

// NewMock will create an instance that implements the Mocker interface
func NewMock(closeErr, execErr error, data ...interface{}) Mocker {
	queries := []MethodsRun{}
	return &mockDb{data, queries, closeErr, execErr}
}

func (r *mockDb) SaveMethodCall(name string, arguments []interface{}) {
	r.methodsRun = append(r.methodsRun, MethodsRun{name, arguments})
}

func (r *mockDb) Backend() interface{} {
	return nil
}

func (r *mockDb) Query(query string, args ...interface{}) (RowsScanner, error) {
	return r.nextScanner()
}

func (r *mockDb) QueryRow(query string, args ...interface{}) Scanner {
	s, _ := r.nextScanner()
	return s
}

func (r *mockDb) QueryValues(query *Query, result ...interface{}) error {
	r.SaveMethodCall("QueryValues", append([]interface{}{query}, result...))
	return QueryValues(r, query, result...)
}

func (r *mockDb) QueryJSON(query string, args ...interface{}) (string, error) {
	r.SaveMethodCall("QueryJSON", append([]interface{}{query}, args...))
	return QueryJSON(r, query, args...)
}

func (r *mockDb) QueryJSONRow(query string, args ...interface{}) (string, error) {
	r.SaveMethodCall("QueryJSONRow", append([]interface{}{query}, args...))
	return QueryJSONRow(r, query, args...)
}

func (r *mockDb) QueryStruct(result interface{}, query string, args ...interface{}) error {
	r.SaveMethodCall("QueryStruct", append([]interface{}{result, query}, args...))
	return QueryStruct(r, result, query, args...)
}

func (r *mockDb) QueryStructRow(result interface{}, query string, args ...interface{}) error {
	return QueryStructRow(r, result, query, args...)
}

func (r *mockDb) QueryWriteCSV(w io.Writer, options CSVOptions, query string, args ...interface{}) error {
	return QueryWriteCSV(w, options, r, query, args...)
}

func (r *mockDb) Close() error {
	return r.closeErr
}

func (r *mockDb) Execute(query string, args ...interface{}) error {
	r.SaveMethodCall("Execute", append([]interface{}{query}, args...))
	return r.execErr
}

func (r *mockDb) QueriesRun() []MethodsRun {
	return r.methodsRun
}

func (r *mockDb) nextScanner() (RowsScanner, error) {
	if len(r.data) == 0 {
		err := errors.New("no mock data found to return")
		return &mockRowsScanner{ErrErr: err}, err
	}
	data := r.data[0]
	r.data = r.data[1:]
	return NewRowsScanner(data), nil
}

func (r *mockDb) VerifyNextCommand(t *testing.T, name string, expected ...interface{}) {
	if len(r.methodsRun) == 0 {
		t.Error("No methods found to have been run")
		return
	}
	current := r.methodsRun[0]
	r.methodsRun = r.methodsRun[1:]
	if current.MethodName != name {
		t.Errorf("Method %s not found. Actual method was %s", name, current.MethodName)
		return
	}
	verifyArgs(t, current.Arguments, expected...)
}

func verifyArgs(t *testing.T, actual []interface{}, expected ...interface{}) {
	if len(expected) != len(actual) {
		t.Fatal("Number of arguments don't match. Expected:", len(expected), "actual:", len(actual))
	}
	for i := range actual {
		if !reflect.DeepEqual(actual[i], expected[i]) {
			t.Errorf("Argument mismatch at %d. Expected:%v, Actual:%v\n", i, expected[i], actual[i])
		}
	}
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

// NewRowsScanner returns a RowsScanner that can scan through a slice of data
func NewRowsScanner(data interface{}) RowsScanner {
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
func (r *mockRowsScanner) Close() error {
	return nil
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

// NewScanner returns a Scanner that can run Scan on a struct or pointer to struct
func NewScanner(data interface{}) Scanner {
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

type errorScanner struct {
	Err error
}

func (s *errorScanner) Scan(dest ...interface{}) error {
	return s.Err
}
