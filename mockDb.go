package onedb

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

type MockDb struct {
	data     []interface{}
	CloseErr error
	ExecErr  error
}

func NewMockDb(data ...interface{}) *MockDb {
	return &MockDb{data: data}
}

func (r *MockDb) QueryJson(query interface{}) (string, error) {
	return r.nextJson()
}

func (r *MockDb) QueryJsonRow(query interface{}) (string, error) {
	return r.nextJson()

}
func (r *MockDb) QueryStruct(query interface{}, result interface{}) error {
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
func (r *MockDb) QueryStructRow(query interface{}, result interface{}) error {
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

func (r *MockDb) Close() error {
	return r.CloseErr
}

func (r *MockDb) Execute(query interface{}) error {
	return r.ExecErr
}

func (r *MockDb) nextJson() (string, error) {
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

func (r *MockDb) nextStruct() (interface{}, error) {
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

type ErrorScanner struct {
	Err     error
}

func (s *ErrorScanner) Scan(dest ...interface{}) error {
	return s.Err
}

