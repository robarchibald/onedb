package onedb

import (
	"testing"
)

func TestQueryJson(t *testing.T) {
	r := NewMockDb("hello", []SimpleData{SimpleData{1, "hello"}})
	json, err := r.QueryJson("select query")
	if json != "hello" {
		t.Error("expected to get back hello", json, err)
	}

	json, err = r.QueryJson("select query2")
	if json != `[{"IntVal":1,"StringVal":"hello"}]` {
		t.Error("expected formatted json back", json, err)
	}

	_, err = r.QueryJson("select query2")
	if err == nil {
		t.Error("expected error after using all the results", err)
	}
}

func TestQueryJsonRow(t *testing.T) {
	r := NewMockDb(SimpleData{1, "hello"})
	json, err := r.QueryJson("select query2")
	if json != `{"IntVal":1,"StringVal":"hello"}` {
		t.Error("expected formatted json back", json, err)
	}

	_, err = r.QueryJsonRow("select query2")
	if err == nil {
		t.Error("expected error after using all the results", err)
	}
}

func TestQueryStruct(t *testing.T) {
	result := []SimpleData{}
	r := NewMockDb()
	err := r.QueryStruct("select query", result)
	if err == nil {
		t.Error("expected error for wrong result type")
	}

	q1 := []SimpleData{SimpleData{1, "hello"}, SimpleData{2, "world"}}
	q2 := []SimpleData{SimpleData{3, "test"}}
	r = NewMockDb(q1, q2)
	err = r.QueryStruct("select query", &result)
	if err != nil || len(result) != 2 || result[0].IntVal != 1 || result[0].StringVal != "hello" || result[1].IntVal != 2 || result[1].StringVal != "world" {
		t.Error("expected 2 valid rows of data", result, err)
	}

	err = r.QueryStruct("select query", &result)
	if err != nil || len(result) != 1 || result[0].IntVal != 3 || result[0].StringVal != "test" {
		t.Error("expected 2 valid row of data", result, err)
	}

	err = r.QueryStruct("select query", &result)
	if err == nil {
		t.Error("expected error after using all the results", err)
	}
}

func TestQueryStructRow(t *testing.T) {
	result := SimpleData{}
	r := NewMockDb()
	err := r.QueryStructRow("select query", result)
	if err == nil {
		t.Error("expected error for wrong result type")
	}

	q1 := SimpleData{1, "hello"}
	q2 := SimpleData{2, "world"}
	r = NewMockDb(q1, q2)
	err = r.QueryStructRow("select query", &result)
	if err != nil || result.IntVal != 1 || result.StringVal != "hello" {
		t.Error("expected valid data", result, err)
	}

	err = r.QueryStructRow("select query", &result)
	if err != nil || result.IntVal != 2 || result.StringVal != "world" {
		t.Error("expected valid data", result, err)
	}

	err = r.QueryStructRow("select query", &result)
	if err == nil {
		t.Error("expected error after using all the results", result, err)
	}
}

func TestSet(t *testing.T) {
	err := set("hello", &SimpleData{1, "test"})
	if err == nil {
		t.Error("expected error due to non-matching types")
	}
}

type SimpleData struct {
	IntVal    int
	StringVal string
}
