package pgx

import (
	"errors"
	"testing"

	"github.com/EndFirstCorp/onedb"
)

func TestBackendConverterQueryJson(t *testing.T) {
	rows := onedb.NewMockRowsScanner([]onedb.SimpleData{onedb.SimpleData{1, "hello"}})
	db := newBackendConverter(&pgxMockBackend{Rows: rows})

	json, err := db.QueryJSON("select * from TestTable")
	if json != "[{\"IntVal\":1,\"StringVal\":\"hello\"}]" {
		t.Error("expected different json back.  Actual:", json, err)
	}
	if err != nil {
		t.Error("didn't expect error")
	}

	db = newBackendConverter(&pgxMockBackend{QueryErr: errors.New("fail")})
	_, err = db.QueryJSON("select * from TestTable")
	if err == nil {
		t.Error("expected error")
	}
}

func TestBackendConverterQueryJsonRow(t *testing.T) {
	rows := onedb.NewMockRowsScanner([]onedb.SimpleData{onedb.SimpleData{1, "hello"}})
	db := newBackendConverter(&pgxMockBackend{Rows: rows})

	json, err := db.QueryJSONRow("select * from TestTable")
	if json != "{\"IntVal\":1,\"StringVal\":\"hello\"}" {
		t.Error("expected different json back.  Actual:", json, err)
	}
	if err != nil {
		t.Error("didn't expect error")
	}

	db = newBackendConverter(&pgxMockBackend{QueryErr: errors.New("fail")})
	_, err = db.QueryJSONRow("select * from TestTable")
	if err == nil {
		t.Error("expected error")
	}
}

func TestBackendConverterQueryStruct(t *testing.T) {
	rows := onedb.NewMockRowsScanner([]onedb.SimpleData{onedb.SimpleData{1, "hello"}})
	db := newBackendConverter(&pgxMockBackend{Rows: rows})
	data := []onedb.SimpleData{}

	// wrong receiver type
	err := db.QueryStruct("query", data)
	if err == nil {
		t.Error("expected error", err)
	}

	// success
	err = db.QueryStruct("query", &data)
	if err != nil || len(data) != 1 || data[0].IntVal != 1 || data[0].StringVal != "hello" {
		t.Error("expected success")
	}

	// query error
	db = newBackendConverter(&pgxMockBackend{QueryErr: errors.New("fail")})
	err = db.QueryStruct("query", &data)
	if err == nil {
		t.Error("expected error", err)
	}
}

func TestBackendConverterQueryStructRow(t *testing.T) {
	rows := onedb.NewMockRowsScanner([]onedb.SimpleData{onedb.SimpleData{1, "hello"}})
	db := newBackendConverter(&pgxMockBackend{Rows: rows})
	data := onedb.SimpleData{}

	// wrong receiver type
	err := db.QueryStructRow("query", data)
	if err == nil {
		t.Error("expected error", err)
	}

	// success
	err = db.QueryStructRow("query", &data)
	if err != nil || data.IntVal != 1 || data.StringVal != "hello" {
		t.Error("expected success")
	}

	// query error
	db = newBackendConverter(&pgxMockBackend{QueryErr: errors.New("fail")})
	err = db.QueryStructRow("query", &data)
	if err == nil {
		t.Error("expected error", err)
	}
}

func TestBackendConverterClose(t *testing.T) {
	db := newBackendConverter(&pgxMockBackend{CloseErr: errors.New("fail")})
	if db.Close() == nil {
		t.Error("expected error on close")
	}

	db = &backendConverter{}
	if db.Close() != nil {
		t.Error("expected nil return when backend is nil")
	}
}

func TestBackendConverterExecute(t *testing.T) {
	db := newBackendConverter(&pgxMockBackend{ExecErr: errors.New("fail")})
	if db.Execute("hi") == nil {
		t.Error("expected error on execute")
	}
}

func TestBackendConverterBackend(t *testing.T) {
	b := &pgxMockBackend{ExecErr: errors.New("fail")}
	db := newBackendConverter(b)
	if b != db.Backend() {
		t.Error("expected to get back my backend")
	}
}

/******************** MOCKS ************************/
// var ErrRowsScannerInvalidData = errors.New("data must be a slice of structs")
// var ErrRowScannerInvalidData = errors.New("data must be a ptr to a struct")

type pgxMockBackend struct {
	Rows        rowsScanner
	Row         scanner
	CloseErr    error
	ExecErr     error
	QueryErr    error
	CopyFromErr error
}

func (b *pgxMockBackend) Close() error {
	return b.CloseErr
}
func (b *pgxMockBackend) Execute(query interface{}) error {
	return b.ExecErr
}
func (b *pgxMockBackend) Query(query interface{}) (rowsScanner, error) {
	return b.Rows, b.QueryErr
}

func (b *pgxMockBackend) QueryRow(query interface{}) scanner {
	if b.QueryErr != nil {
		return &onedb.MockScanner{ScanErr: b.QueryErr}
	}
	return b.Row
}

//fixme
func (b *pgxMockBackend) CopyFrom(tableName string, columnNames []string, rows [][]interface{}) (int, error) {
	err := b.CopyFromErr
	copyCount := 10
	return copyCount, err
}

// type mockRowsScanner struct {
// 	sliceValue reflect.Value
// 	sliceLen   int
// 	structType reflect.Type
// 	structLen  int
// 	data       interface{}
// 	currentRow int
// 	ColumnsErr error
// 	CloseErr   error
// 	ScanErr    error
// 	ErrErr     error
// }

// func newMockRowsScanner(data interface{}) *mockRowsScanner {
// 	if data == nil || reflect.TypeOf(data).Kind() != reflect.Slice || reflect.TypeOf(data).Elem().Kind() != reflect.Struct {
// 		return &mockRowsScanner{ScanErr: ErrRowsScannerInvalidData, ErrErr: ErrRowsScannerInvalidData}
// 	}
// 	sliceValue := reflect.ValueOf(data)
// 	sliceLen := sliceValue.Len()
// 	structType := reflect.TypeOf(data).Elem()
// 	structLen := structType.NumField()

// 	return &mockRowsScanner{data: data, currentRow: -1, sliceValue: sliceValue, sliceLen: sliceLen, structType: structType, structLen: structLen}
// }

// func (r *mockRowsScanner) Columns() ([]string, error) {
// 	if r.ColumnsErr != nil {
// 		return nil, r.ColumnsErr
// 	}

// 	columns := make([]string, r.structLen)
// 	for i := 0; i < r.structLen; i++ {
// 		columns[i] = r.structType.Field(i).Name
// 	}
// 	return columns, nil
// }

// func (r *mockRowsScanner) Next() bool {
// 	r.currentRow++
// 	if r.currentRow >= r.sliceLen {
// 		return false
// 	}
// 	return true
// }
// func (r *mockRowsScanner) Close() error {
// 	return r.CloseErr
// }
// func (r *mockRowsScanner) Scan(dest ...interface{}) error {
// 	if r.ScanErr != nil {
// 		return r.ScanErr
// 	}
// 	if r.currentRow >= r.sliceLen || r.currentRow < 0 {
// 		return errors.New("invalid current row")
// 	}
// 	return setDestValue(r.sliceValue.Index(r.currentRow), dest)
// }

// func setDestValue(structVal reflect.Value, dest []interface{}) error {
// 	if len(dest) != structVal.NumField() {
// 		return fmt.Errorf("expected equal number of dest values as source. Expected: %d, Actual: %d", structVal.NumField(), len(dest))
// 	}
// 	for i := range dest {
// 		destination := reflect.ValueOf(dest[i]).Elem()
// 		source := structVal.Field(i)
// 		if destination.Type() != source.Type() && destination.Type().Kind() != reflect.Interface {
// 			return fmt.Errorf("source and destination types do not match at index: %d", i)
// 		}
// 		destination.Set(source)
// 	}
// 	return nil
// }

// func (r *mockRowsScanner) Err() error {
// 	return r.ErrErr
// }

// type mockScanner struct {
// 	structValue reflect.Value
// 	data        interface{}
// 	ScanErr     error
// }

// func newMockScanner(data interface{}) *mockScanner {
// 	if data == nil || reflect.TypeOf(data).Kind() != reflect.Ptr || reflect.TypeOf(data).Elem().Kind() != reflect.Struct {
// 		return &mockScanner{ScanErr: ErrRowScannerInvalidData}
// 	}
// 	structValue := reflect.ValueOf(data).Elem()
// 	return &mockScanner{data: data, structValue: structValue}
// }

// func (s *mockScanner) Scan(dest ...interface{}) error {
// 	if s.ScanErr != nil {
// 		return s.ScanErr
// 	}
// 	return setDestValue(s.structValue, dest)
// }
