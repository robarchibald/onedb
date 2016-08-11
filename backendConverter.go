package onedb

import (
	"errors"
	"reflect"
)

type BackendConverter struct {
	backend BackendConnecter
	OneDBer
}

func NewBackendConverter(backend BackendConnecter) *BackendConverter {
	return &BackendConverter{backend: backend}
}

func (c *BackendConverter) QueryJson(query interface{}) (string, error) {
	rows, err := c.backend.Query(query)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	return getJson(rows)
}

func (c *BackendConverter) QueryJsonRow(query interface{}) (string, error) {
	rows, err := c.backend.Query(query)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	return getJsonRow(rows)
}

func (c *BackendConverter) QueryStruct(query interface{}, result interface{}) error {
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

func (c *BackendConverter) QueryStructRow(query interface{}, result interface{}) error {
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

func (c *BackendConverter) Close() error {
	if c.backend != nil {
		return c.backend.Close()
	}
	return nil
}

func (c *BackendConverter) Execute(query interface{}) error {
	return c.backend.Execute(query)
}
