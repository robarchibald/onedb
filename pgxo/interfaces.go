package pgxo

type rowsScanner interface {
	Columns() ([]string, error)
	Next() bool
	Close() error
	Err() error
	scanner
}

type scanner interface {
	Scan(dest ...interface{}) error
}

type DBer interface {
	Backend() interface{}
	Close() error
	Execute(query interface{}) error
	QueryValues(query interface{}, result ...interface{}) error
	QueryJSON(query interface{}) (string, error)
	QueryJSONRow(query interface{}) (string, error)
	QueryStruct(query interface{}, result interface{}) error
	QueryStructRow(query interface{}, result interface{}) error
	CopyFrom(tableName interface{}, columnNames []string, rowSrc [][]interface{}) (int, error)
	// CopyFrom(tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int, error)
}
