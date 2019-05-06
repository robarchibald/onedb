package onedb

import (
	"errors"
	"testing"
)

func TestQueryJson(t *testing.T) {
	rows := NewRowsScanner([]SimpleData{SimpleData{1, "hello"}})
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

func TestQueryJsonRow(t *testing.T) {
	rows := NewRowsScanner([]SimpleData{SimpleData{1, "hello"}})
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

func TestQueryStruct(t *testing.T) {
	rows := NewRowsScanner([]SimpleData{SimpleData{1, "hello"}})
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

func TestQueryStructRow(t *testing.T) {
	rows := NewRowsScanner([]SimpleData{SimpleData{1, "hello"}})
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

func TestNewQuery(t *testing.T) {
	q := NewQuery("query", "arg1", "arg2")
	if q == nil || q.Query != "query" || len(q.Args) != 2 || q.Args[0] != "arg1" || q.Args[1] != "arg2" {
		t.Error("expected success")
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
