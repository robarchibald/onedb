package onedb

import (
	"errors"
	"testing"
)

func TestQueryJson(t *testing.T) {
	d := NewMock("hello", []SimpleData{SimpleData{1, "hello"}})
	json, err := d.QueryJson("select query")
	if json != "hello" {
		t.Error("expected to get back hello", json, err)
	}

	json, err = d.QueryJson("select query2")
	if json != `[{"IntVal":1,"StringVal":"hello"}]` {
		t.Error("expected formatted json back", json, err)
	}

	_, err = d.QueryJson("select query2")
	if err == nil {
		t.Error("expected error after using all the results", err)
	}
}

func TestQueryJsonRow(t *testing.T) {
	d := NewMock(SimpleData{1, "hello"})
	json, err := d.QueryJson("select query2")
	if json != `{"IntVal":1,"StringVal":"hello"}` {
		t.Error("expected formatted json back", json, err)
	}

	_, err = d.QueryJsonRow("select query2")
	if err == nil {
		t.Error("expected error after using all the results", err)
	}
}

func TestQueryStruct(t *testing.T) {
	result := []SimpleData{}
	d := NewMock()
	err := d.QueryStruct("select query", result)
	if err == nil {
		t.Error("expected error for wrong result type")
	}

	q1 := []SimpleData{SimpleData{1, "hello"}, SimpleData{2, "world"}}
	q2 := []SimpleData{SimpleData{3, "test"}}
	d = NewMock(q1, q2)
	err = d.QueryStruct("select query", &result)
	if err != nil || len(result) != 2 || result[0].IntVal != 1 || result[0].StringVal != "hello" || result[1].IntVal != 2 || result[1].StringVal != "world" {
		t.Error("expected 2 valid rows of data", result, err)
	}

	err = d.QueryStruct("select query", &result)
	if err != nil || len(result) != 1 || result[0].IntVal != 3 || result[0].StringVal != "test" {
		t.Error("expected 2 valid row of data", result, err)
	}

	err = d.QueryStruct("select query", &result)
	if err == nil {
		t.Error("expected error after using all the results", err)
	}
}

func TestQueryStructRow(t *testing.T) {
	result := SimpleData{}
	d := NewMock()
	err := d.QueryStructRow("select query", result)
	if err == nil {
		t.Error("expected error for wrong result type")
	}

	q1 := SimpleData{1, "hello"}
	q2 := SimpleData{2, "world"}
	d = NewMock(q1, q2)
	err = d.QueryStructRow("select query", &result)
	if err != nil || result.IntVal != 1 || result.StringVal != "hello" {
		t.Error("expected valid data", result, err)
	}

	err = d.QueryStructRow("select query", &result)
	if err != nil || result.IntVal != 2 || result.StringVal != "world" {
		t.Error("expected valid data", result, err)
	}

	err = d.QueryStructRow("select query", &result)
	if err == nil {
		t.Error("expected error after using all the results", result, err)
	}
}

func TestSet(t *testing.T) {
	err := setDest("hello", &SimpleData{1, "test"})
	if err == nil {
		t.Error("expected error due to non-matching types")
	}
}

func TestClose(t *testing.T) {
	err := errors.New("fail")
	d := &MockDb{CloseErr: err}
	if d.Close() != err {
		t.Error("expected error")
	}
}

func TestExec(t *testing.T) {
	err := errors.New("fail")
	d := &MockDb{ExecErr: err}
	if d.Execute("query") != err {
		t.Error("expected error")
	}
}

type SimpleData struct {
	IntVal    int
	StringVal string
}
