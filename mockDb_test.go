package onedb

import (
	"errors"
	"testing"
)

func TestMockDBQueryJson(t *testing.T) {
	d := NewMock(nil, nil, "hello", []SimpleData{SimpleData{1, "hello"}})
	json, err := d.QueryJSON("select query")
	if json != "hello" {
		t.Error("expected to get back hello", json, err)
	}

	json, err = d.QueryJSON("select query2")
	if json != `[{"IntVal":1,"StringVal":"hello"}]` {
		t.Error("expected formatted json back", json, err)
	}

	_, err = d.QueryJSON("select query2")
	if err == nil {
		t.Error("expected error after using all the results", err)
	}
}

func TestMockDBQueryJsonRow(t *testing.T) {
	d := NewMock(nil, nil, SimpleData{1, "hello"})
	json, err := d.QueryJSON("select query2")
	if json != `{"IntVal":1,"StringVal":"hello"}` {
		t.Error("expected formatted json back", json, err)
	}

	_, err = d.QueryJSONRow("select query2")
	if err == nil {
		t.Error("expected error after using all the results", err)
	}
}

func TestMockDBQueryStruct(t *testing.T) {
	result := []SimpleData{}
	d := NewMock(nil, nil)
	err := d.QueryStruct(result, "select query")
	if err == nil {
		t.Error("expected error for wrong result type")
	}

	q1 := []SimpleData{SimpleData{1, "hello"}, SimpleData{2, "world"}}
	q2 := []SimpleData{SimpleData{3, "test"}}
	d = NewMock(nil, nil, q1, q2)
	err = d.QueryStruct(&result, "select query")
	if err != nil || len(result) != 2 || result[0].IntVal != 1 || result[0].StringVal != "hello" || result[1].IntVal != 2 || result[1].StringVal != "world" {
		t.Error("expected 2 valid rows of data", result, err)
	}

	err = d.QueryStruct(&result, "select query")
	if err != nil || len(result) != 1 || result[0].IntVal != 3 || result[0].StringVal != "test" {
		t.Error("expected 2 valid row of data", result, err)
	}

	err = d.QueryStruct(&result, "select query")
	if err == nil {
		t.Error("expected error after using all the results", err)
	}
}

func TestMockDBQueryStructRow(t *testing.T) {
	result := SimpleData{}
	d := NewMock(nil, nil)
	err := d.QueryStructRow(result, "select query")
	if err == nil {
		t.Error("expected error for wrong result type")
	}

	q1 := SimpleData{1, "hello"}
	q2 := SimpleData{2, "world"}
	d = NewMock(nil, nil, q1, q2)
	err = d.QueryStructRow(&result, "select query")
	if err != nil || result.IntVal != 1 || result.StringVal != "hello" {
		t.Error("expected valid data", result, err)
	}

	err = d.QueryStructRow(&result, "select query")
	if err != nil || result.IntVal != 2 || result.StringVal != "world" {
		t.Error("expected valid data", result, err)
	}

	err = d.QueryStructRow(&result, "select query")
	if err == nil {
		t.Error("expected error after using all the results", result, err)
	}
}

func TestSetDest(t *testing.T) {
	err := setDest("hello", &SimpleData{1, "test"})
	if err == nil {
		t.Error("expected error due to non-matching types")
	}
}

func TestClose(t *testing.T) {
	err := errors.New("fail")
	d := &mockDb{closeErr: err}
	if d.Close() != err {
		t.Error("expected error")
	}
}

func TestBackend(t *testing.T) {
	d := &mockDb{}
	if d.Backend() != nil {
		t.Error("expected no backend")
	}
}

func TestExec(t *testing.T) {
	err := errors.New("fail")
	d := &mockDb{execErr: err}
	if d.Execute("query") != err {
		t.Error("expected error")
	}
}

func TestErrorScannerScan(t *testing.T) {
	err := errors.New("fail")
	e := errorScanner{err}
	if e.Scan() != err {
		t.Error("expected errorScanner to return error")
	}
}

type SimpleData struct {
	IntVal    int
	StringVal string
}
