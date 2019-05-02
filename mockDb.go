package onedb

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

type mockDb struct {
	data     []interface{}
	queries  []interface{}
	closeErr error
	execErr  error
}

type MockDBer interface {
	DBer
	QueriesRun() []interface{}
}

func NewMock(closeErr, execErr error, data ...interface{}) MockDBer {
	queries := []interface{}{}
	return &mockDb{data, queries, closeErr, execErr}
}

func (r *mockDb) Backend() interface{} {
	return nil
}

func (r *mockDb) QueryValues(query interface{}, result ...interface{}) error {
	r.queries = append(r.queries, query)
	return nil
}

func (r *mockDb) QueryJSON(query interface{}) (string, error) {
	r.queries = append(r.queries, query)
	return r.nextJSON()
}

func (r *mockDb) QueryJSONRow(query interface{}) (string, error) {
	r.queries = append(r.queries, query)
	return r.nextJSON()
}

func (r *mockDb) QueryStruct(query interface{}, result interface{}) error {
	r.queries = append(r.queries, query)
	resultType := reflect.TypeOf(result)
	if resultType.Kind() != reflect.Ptr || resultType.Elem().Kind() != reflect.Slice || resultType.Elem().Elem().Kind() != reflect.Struct {
		return errors.New("result must be a pointer to a slice of structs")
	}
	data, err := r.nextStruct()
	if err != nil {
		return err
	}
	return setDest(data, result)
}

func (r *mockDb) QueryStructRow(query interface{}, result interface{}) error {
	r.queries = append(r.queries, query)
	resultType := reflect.TypeOf(result)
	if resultType.Kind() != reflect.Ptr || resultType.Elem().Kind() != reflect.Struct {
		return errors.New("result must be a pointer to a struct")
	}
	data, err := r.nextStruct()
	if err != nil {
		return err
	}
	return setDest(data, result)
}

func (r *mockDb) Close() error {
	return r.closeErr
}

func (r *mockDb) Execute(query interface{}) error {
	r.queries = append(r.queries, query)
	return r.execErr
}

func (r *mockDb) QueriesRun() []interface{} {
	return r.queries
}

func (r *mockDb) nextJSON() (string, error) {
	data, err := r.nextStruct()
	if err != nil {
		return "", err
	}

	dataStr, ok := data.(string)
	if ok {
		return dataStr, nil
	}

	output, err := json.Marshal(data)
	return string(output), err
}

func (r *mockDb) nextStruct() (interface{}, error) {
	if len(r.data) == 0 {
		return "", errors.New("no mock data found to return")
	}
	data := r.data[0]
	r.data = r.data[1:]
	return data, nil
}

func setDest(source interface{}, dest interface{}) error {
	sourceType := reflect.TypeOf(source)
	destType := reflect.TypeOf(dest)
	if sourceType != destType.Elem() {
		return fmt.Errorf("expected types to match. source: %v, dest: %v", sourceType, destType)
	}

	destValue := reflect.ValueOf(dest)
	sourceValue := reflect.ValueOf(source)
	destValue.Elem().Set(sourceValue)
	return nil
}
