package onedb

import (
	"reflect"

	"github.com/pkg/errors"
)

// QueryValues runs a query against the provided Backender and populates result values
func QueryValues(backend Backender, query *SqlQuery, result ...interface{}) error {
	if query == nil {
		return ErrQueryIsNil
	}
	row := backend.QueryRow(query.Query, query.Args)
	return row.Scan(result...)
}

// QueryJSON runs a query against the provided Backender and returns the JSON result
func QueryJSON(backend Backender, query string, args ...interface{}) (string, error) {
	rows, err := backend.Query(query, args...)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	return GetJSON(rows)
}

// QueryJSONRow runs a query against the provided Backender and returns the JSON result
func QueryJSONRow(backend Backender, query string, args ...interface{}) (string, error) {
	rows, err := backend.Query(query, args...)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	return GetJSONRow(rows)
}

// QueryStruct runs a query against the provided Backender and populates the provided result
func QueryStruct(backend Backender, result interface{}, query string, args ...interface{}) error {
	resultType := reflect.TypeOf(result)
	if !isPointer(resultType) || !isSlice(resultType.Elem()) {
		return errors.New("Invalid result argument.  Must be a pointer to a slice")
	}

	rows, err := backend.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	return GetStruct(rows, result)
}

// QueryStructRow runs a query against the provided Backender and populates the provided result
func QueryStructRow(backend Backender, result interface{}, query string, args ...interface{}) error {
	if !isPointer(reflect.TypeOf(result)) {
		return errors.New("Invalid result argument.  Must be a pointer to a struct")
	}

	rows, err := backend.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	return GetStructRow(rows, result)
}

func isPointer(item reflect.Type) bool {
	return item.Kind() == reflect.Ptr
}

func isSlice(item reflect.Type) bool {
	return item.Kind() == reflect.Slice
}
