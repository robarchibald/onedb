package onedb

type BackendConnecter interface {
	Close() error
	Execute(query interface{}) error
	Query(query interface{}) (RowsScanner, error)
	QueryRow(query interface{}) RowScanner
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

type OneDBer interface {
	Close() error
	Execute(query interface{}) error
	QueryJson(query interface{}) (string, error)
	QueryJsonRow(query interface{}) (string, error)
	QueryStruct(query interface{}, result interface{}) error
	QueryStructRow(query interface{}, result interface{}) error
}
