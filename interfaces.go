package testableDb

type BackendConnecter interface {
	Close() error
	Execute(query string, args ...interface{}) error
	Query(query string, args ...interface{}) (RowsScanner, error)
	QueryRow(query string, args ...interface{}) RowScanner
}

type RowsScanner interface {
	Columns() ([]string, error)
	Next() bool
	Close() error
	Scan(dest ...interface{}) error
	Err() error
}

type RowScanner interface {
	Scan(dest ...interface{}) error
}
