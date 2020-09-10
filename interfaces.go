package onedb

import (
	"io"

	"github.com/pkg/errors"
)

// Backender is the db interface needed by onedb to enable QueryStruct and QueryJSON capability
type Backender interface {
	Query(query string, args ...interface{}) (RowsScanner, error)
	QueryRow(query string, args ...interface{}) Scanner
}

// RowsScanner is the rows interface needed by onedb to enable QueryStruct and QueryJSON capability
type RowsScanner interface {
	Close() error
	Columns() ([]string, error)
	Next() bool
	Err() error
	Scanner
}

// Scanner is the row interface needed by onedb to enable QueryStruct and QueryJSON capability
type Scanner interface {
	Scan(dest ...interface{}) error
}

// DBer is the added interface that onedb can enable for database querying
type DBer interface {
	QueryValues(query *Query, result ...interface{}) error
	QueryJSON(query string, args ...interface{}) (string, error)
	QueryJSONRow(query string, args ...interface{}) (string, error)
	QueryStruct(result interface{}, query string, args ...interface{}) error
	QueryStructRow(result interface{}, query string, args ...interface{}) error
	QueryWriteCSV(w io.Writer, options CSVOptions, query string, args ...interface{}) error
}

// ErrRowsScannerInvalidData occurs when the provided data is not a slice of type struct.
var ErrRowsScannerInvalidData = errors.New("data must be a slice of structs")

// ErrRowScannerInvalidData occurs when the provided data is not a pointer to a struct.
var ErrRowScannerInvalidData = errors.New("data must be a ptr to a struct")

// ErrQueryIsNil occurs when the provided query is invalid.
var ErrQueryIsNil = errors.New("invalid query")
