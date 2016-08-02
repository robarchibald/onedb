package testableDb

import (
	"errors"
	"testing"
)

func TestMockBackendClose(t *testing.T) {
	err := errors.New("test")
	b := MockBackend{CloseErr: err}
	if b.Close() != err {
		t.Error("expected my error when calling close")
	}
}

func TestMockBackendExecute(t *testing.T) {
	err := errors.New("test")
	b := MockBackend{ExecErr: err}
	if b.Execute("delete * from table") != err {
		t.Error("expected my error when calling execute")
	}
}

func TestMockBackendQuery(t *testing.T) {
	expectedErr := errors.New("test")
	expectedRows := &MockRowsScanner{}
	b := MockBackend{QueryErr: expectedErr, Rows: expectedRows}
	rows, err := b.Query("select * from test")
	if rows != expectedRows || err != expectedErr {
		t.Error("expected to get the rows and error I entered")
	}
}

func TestMockBackendQueryRow(t *testing.T) {
	expectedRow := &MockRowScanner{}
	b := MockBackend{Row: expectedRow}
	row := b.QueryRow("select * from test")
	if row != expectedRow {
		t.Error("expected to get the row I entered")
	}
}
func TestNewMockRowsScanner(t *testing.T) {
	rows := NewMockRowsScanner(nil)
	if rows.Err() != ErrRowsScannerInvalidData {
		t.Error("Expected error with empty data passed in")
	}

	rows = NewMockRowsScanner(1234)
	if rows.Err() != ErrRowsScannerInvalidData {
		t.Error("Expected error with non-slice data passed in")
	}

	rows = NewMockRowsScanner([]string{"bogus", "data"})
	if rows.Err() != ErrRowsScannerInvalidData {
		t.Error("Expected error with slice of non-struct data passed in")
	}

	data := []SimpleData{}
	rows = NewMockRowsScanner(data)
	if rows.Err() != nil {
		t.Error("Expected no error with valid data passed in")
	}
}

func TestMockRowsScannerColumns(t *testing.T) {
	expectedErr := errors.New("test")
	rows := &MockRowsScanner{ColumnsErr: expectedErr}
	_, err := rows.Columns()
	if err != expectedErr {
		t.Error("expected columns to use my error")
	}

	rows = NewMockRowsScanner([]SimpleData{})
	cols, err := rows.Columns()
	if err != nil || len(cols) != 2 || cols[0] != "IntVal" || cols[1] != "StringVal" {
		t.Error("expected colums to find fields in SimpleData", cols, err)
	}
}

func TestMockRowsScannerNext(t *testing.T) {
	rows := NewMockRowsScanner([]SimpleData{}) // no rows
	if rows.Next() || rows.currentRow != 0 {
		t.Error("expected Next() to return false")
	}

	rows = NewMockRowsScanner([]SimpleData{SimpleData{1, "hello"}, SimpleData{2, "world"}}) // 2 rows
	next1 := rows.Next()
	next2 := rows.Next()
	next3 := rows.Next()
	if !next1 || !next2 || next3 || rows.currentRow != 2 {
		t.Error("expected true, true, false", next1, next2, next3, rows.currentRow)
	}
}

func TestMockRowsScannerClose(t *testing.T) {
	err := errors.New("test")
	rows := MockRowsScanner{CloseErr: err}
	if rows.Close() != err {
		t.Error("Expected close to fail with error")
	}
}

func TestMockRowsScannerScan(t *testing.T) {
	var intVal1, intVal2 int
	var strVal1, strVal2 string

	// scan error
	err := errors.New("test")
	rows := &MockRowsScanner{ScanErr: err}
	if rows.Scan() != err {
		t.Error("expected scan error")
	}

	// don't call Next() first
	rows = NewMockRowsScanner([]SimpleData{SimpleData{1, "hello"}})
	err = rows.Scan(&intVal1, &strVal1)
	if err == nil || err.Error() != "invalid current row" {
		t.Error("expected invalid current row. Actual:", err)
	}

	// mismatched number of arguments vs. struct
	rows.Next()
	err = rows.Scan(&intVal1)
	if err == nil || err.Error() != "expected equal number of dest values as source. Expected: 2, Actual: 1" {
		t.Error("expected scan error. Actual:", err)
	}

	// invalid argument types
	err = rows.Scan(&strVal1, &intVal1)
	if err == nil || err.Error() != "source and destination types do not match at index: 0" {
		t.Error("expected scan error. Actual:", err)
	}

	// currentRow is > number of rows
	rows.Next()
	err = rows.Scan(&intVal1, &strVal1)
	if err == nil || err.Error() != "invalid current row" {
		t.Error("expected current row number to be beyond number of rows")
	}

	// success!
	rows = NewMockRowsScanner([]SimpleData{SimpleData{1, "hello"}, SimpleData{2, "world"}}) // 2 rows
	rows.Next()
	rows.Scan(&intVal1, &strVal1)
	rows.Next()
	rows.Scan(&intVal2, &strVal2)
	if intVal1 != 1 || intVal2 != 2 || strVal1 != "hello" || strVal2 != "world" {
		t.Error("expected 1, 2, hello, world.  Actual:", intVal1, intVal2, strVal1, strVal2)
	}
}

func TestMockRowsScannerErr(t *testing.T) {
	err := errors.New("test")
	rows := MockRowsScanner{ErrErr: err}
	if rows.Err() != err {
		t.Error("expected error to match what I entered")
	}
}

func TestNewMockRowScanner(t *testing.T) {
	row := NewMockRowScanner(nil)
	if row.ScanErr != ErrRowScannerInvalidData {
		t.Error("Expected error with empty data passed in")
	}

	row = NewMockRowScanner(1234)
	if row.ScanErr != ErrRowScannerInvalidData {
		t.Error("Expected error with non-slice data passed in")
	}

	row = NewMockRowScanner([]string{"bogus", "data"})
	if row.ScanErr != ErrRowScannerInvalidData {
		t.Error("Expected error with slice of non-struct data passed in")
	}

	data := &SimpleData{1, "hello"}
	row = NewMockRowScanner(data)
	if row.ScanErr != nil {
		t.Error("Expected no error with valid data passed in")
	}
}

func TestRowScannerScan(t *testing.T) {
	var intVal int
	var strVal string

	// scan error
	err := errors.New("test")
	row := &MockRowScanner{ScanErr: err}
	if row.Scan() != err {
		t.Error("expected scan error")
	}

	// success
	data := &SimpleData{1, "hello"}
	row = NewMockRowScanner(data)
	if row.Scan(&intVal, &strVal) != nil || intVal != 1 || strVal != "hello" {
		t.Error("expected scan success")
	}
}

type SimpleData struct {
	IntVal    int
	StringVal string
}
