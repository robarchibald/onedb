package onedb

import (
	"errors"
	"reflect"
)

type backender interface {
	Close() error
	Execute(query interface{}) error
	Query(query interface{}) (rowsScanner, error)
	QueryRow(query interface{}) scanner
}

type backendConverter struct {
	backend backender
	DBer
}

func newBackendConverter(backend backender) DBer {
	return &backendConverter{backend: backend}
}

func (c *backendConverter) Backend() interface{} {
	return c.backend
}

func (c *backendConverter) QueryJSON(query interface{}) (string, error) {
	rows, err := c.backend.Query(query)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	return getJSON(rows)
}

func (c *backendConverter) QueryJSONRow(query interface{}) (string, error) {
	rows, err := c.backend.Query(query)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	return getJSONRow(rows)
}

func (c *backendConverter) QueryStruct(query interface{}, result interface{}) error {
	resultType := reflect.TypeOf(result)
	if !isPointer(resultType) || !isSlice(resultType.Elem()) {
		return errors.New("Invalid result argument.  Must be a pointer to a slice")
	}

	rows, err := c.backend.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	return getStruct(rows, result)
}

func (c *backendConverter) QueryStructRow(query interface{}, result interface{}) error {
	if !isPointer(reflect.TypeOf(result)) {
		return errors.New("Invalid result argument.  Must be a pointer to a struct")
	}

	rows, err := c.backend.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	return getStructRow(rows, result)
}

func (c *backendConverter) Close() error {
	if c.backend != nil {
		return c.backend.Close()
	}
	return nil
}

func (c *backendConverter) Execute(query interface{}) error {
	return c.backend.Execute(query)
}

func isPointer(item reflect.Type) bool {
	return item.Kind() == reflect.Ptr
}

func isSlice(item reflect.Type) bool {
	return item.Kind() == reflect.Slice
}
