package onedb

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
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

// MockDBer is a fake database that can be used in place of a pgx or sql lib database for testing
type MockDBer interface {
	DBer
	QueriesRun() []MethodsRun
}

// NewMock will create an instance that implements the MockDBer interface
func NewMock(closeErr, execErr error, data ...interface{}) MockDBer {
	queries := []MethodsRun{}
	return &mockDb{data, queries, closeErr, execErr}
}

func (r *mockDb) SaveMethodCall(name string, arguments []interface{}) {
	r.methodsRun = append(r.methodsRun, MethodsRun{name, arguments})
}

func (r *mockDb) Backend() interface{} {
	return nil
}

func (r *mockDb) QueryValues(query *Query, result ...interface{}) error {
	r.SaveMethodCall("QueryValues", append([]interface{}{query}, result...))
	return nil
}

func (r *mockDb) QueryJSON(query string, args ...interface{}) (string, error) {
	r.SaveMethodCall("QueryJSON", append([]interface{}{query}, args...))
	return r.nextJSON()
}

func (r *mockDb) QueryJSONRow(query string, args ...interface{}) (string, error) {
	r.SaveMethodCall("QueryJSONRow", append([]interface{}{query}, args...))
	return r.nextJSON()
}

func (r *mockDb) QueryStruct(result interface{}, query string, args ...interface{}) error {
	r.SaveMethodCall("QueryStruct", append([]interface{}{result, query}, args...))

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

func (r *mockDb) QueryStructRow(result interface{}, query string, args ...interface{}) error {
	r.SaveMethodCall("QueryStructRow", append([]interface{}{result, query}, args...))

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

func (r *mockDb) Execute(query string, args ...interface{}) error {
	r.SaveMethodCall("Execute", append([]interface{}{query}, args...))
	return r.execErr
}

func (r *mockDb) QueriesRun() []MethodsRun {
	return r.methodsRun
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

type errorScanner struct {
	Err error
}

func (s *errorScanner) Scan(dest ...interface{}) error {
	return s.Err
}
