package onedb

import (
	"io"
	"net"
	"reflect"
	"time"

	// "github.com/EndFirstCorp/onedb"
	"github.com/pkg/errors"
)

// QueryValues runs a query against the provided Backender and populates result values
func QueryValues(backend Backender, query *Query, result ...interface{}) error {
	if query == nil {
		return ErrQueryIsNil
	}
	row := backend.QueryRow(query.Query, query.Args...)
	return row.Scan(result...)
}

// QueryJSON runs a query against the provided Backender and returns the JSON result
func QueryJSON(backend Backender, query string, args ...interface{}) (string, error) {
	rows, err := backend.Query(query, args...)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	return getJSON(rows)
}

// QueryJSONRow runs a query against the provided Backender and returns the JSON result
func QueryJSONRow(backend Backender, query string, args ...interface{}) (string, error) {
	rows, err := backend.Query(query, args...)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	return getJSONRow(rows)
}

// QueryStruct runs a query against the provided Backender and populates the provided result
func QueryStruct(backend Backender, result interface{}, query string, args ...interface{}) error {
	resultType := reflect.TypeOf(result)
	if !IsPointer(resultType) || !IsSlice(resultType.Elem()) {
		return errors.New("Invalid result argument.  Must be a pointer to a slice")
	}

	rows, err := backend.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	return getStruct(rows, result)
}

// QueryStructRow runs a query against the provided Backender and populates the provided result
func QueryStructRow(backend Backender, result interface{}, query string, args ...interface{}) error {
	if !IsPointer(reflect.TypeOf(result)) {
		return errors.New("Invalid result argument.  Must be a pointer to a struct")
	}

	rows, err := backend.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	return getStructRow(rows, result)
}

// IsPointer is used to determine if a reflect.Type is a pointer
func IsPointer(item reflect.Type) bool {
	return item.Kind() == reflect.Ptr
}

// IsSlice is used to determine if a reflect.Type is a slice
func IsSlice(item reflect.Type) bool {
	return item.Kind() == reflect.Slice
}

// IsStruct is used to determine if a reflect.Type is a struct
func IsStruct(item reflect.Type) bool {
	return item.Kind() == reflect.Struct
}

// QueryWriteCSV runs a query against the provided Backender and saves the response to the specified file in CSV format
func QueryWriteCSV(w io.Writer, options map[string]bool, backend Backender, query string, args ...interface{}) error {
	rows, err := backend.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	return writeCSV(rows, w, options)
}

// Query is a generic struct that houses a query string and arguments used to construct a query
type Query struct {
	Query string
	Args  []interface{}
}

// NewQuery is tne constructor for a Query struct
func NewQuery(query string, args ...interface{}) *Query {
	return &Query{Query: query, Args: args}
}

// DialFunc is the shape of the function used to dial a TCP connection
type DialFunc func(network, addr string) (net.Conn, error)

// NewMockDialer returns a onedb.DialFunc for testing purposes
func NewMockDialer(err error) DialFunc {
	return func(network, addr string) (net.Conn, error) {
		return nil, err
	}
}

// DialTCP is a helper function that will dial a TCP port and set a 2 minute time period
func DialTCP(network, addr string) (net.Conn, error) {
	tcpAddr, err := net.ResolveTCPAddr(network, addr)
	if err != nil {
		return nil, err
	}
	tc, err := net.DialTCP(network, nil, tcpAddr)
	if err != nil {
		return nil, err
	}
	if err := tc.SetKeepAlive(true); err != nil {
		return nil, err
	}
	if err := tc.SetKeepAlivePeriod(2 * time.Minute); err != nil {
		return nil, err
	}
	return tc, nil
}
