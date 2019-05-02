package pgxo

import (
	"fmt"
	"reflect"

	"github.com/EndFirstCorp/onedb"
	"github.com/pkg/errors"
)

type backender interface {
	Close() error
	Execute(query interface{}) error
	Query(query interface{}) (rowsScanner, error)
	QueryRow(query interface{}) scanner
	CopyFrom(tableName string, columnNames []string, rowSrc [][]interface{}) (int, error)
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

func (c *backendConverter) QueryValues(query interface{}, result ...interface{}) error {
	row := c.backend.QueryRow(query)
	return row.Scan(result...)
}

func (c *backendConverter) QueryJSON(query interface{}) (string, error) {
	rows, err := c.backend.Query(query)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	return onedb.GetJSON(rows)
}

func (c *backendConverter) QueryJSONRow(query interface{}) (string, error) {
	rows, err := c.backend.Query(query)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	return onedb.GetJSONRow(rows)
}

func (c *backendConverter) QueryStruct(query interface{}, result interface{}) error {
	resultType := reflect.TypeOf(result)
	if !onedb.IsPointer(resultType) || !onedb.IsSlice(resultType.Elem()) {
		return errors.New("Invalid result argument.  Must be a pointer to a slice")
	}

	rows, err := c.backend.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	return onedb.GetStruct(rows, result)
}

func (c *backendConverter) QueryStructRow(query interface{}, result interface{}) error {
	if !onedb.IsPointer(reflect.TypeOf(result)) {
		return errors.New("Invalid result argument.  Must be a pointer to a struct")
	}

	rows, err := c.backend.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	return onedb.GetStructRow(rows, result)
}

func (c *backendConverter) Copy(tableName string, columnNames []string, rowSrc [][]interface{}) (int, error) {
	fmt.Println("copy before")
	// fmt.Println("copy before, tableName:", tableName, "rowSrc:", rowSrc, "columnNames:", columnNames)
	copyCount, err := c.backend.CopyFrom(tableName, columnNames, rowSrc)
	fmt.Println("copy after, rowsCopied:", copyCount, "err:", err)
	return copyCount, err
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

// func isPointer(item reflect.Type) bool {
// 	return item.Kind() == reflect.Ptr
// }

// func isSlice(item reflect.Type) bool {
// 	return item.Kind() == reflect.Slice
// }
