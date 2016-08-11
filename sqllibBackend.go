package onedb

import (
	"database/sql"
	"errors"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
)

var ErrInvalidQueryType error = errors.New("Invalid query. Must be of type *SqlQuery")
var sqlOpen Opener = &SqllibOpener{}

type Opener interface {
	Open(driverName, dataSourceName string) (SqlLibBackender, error)
}

type SqllibOpener struct{}

func (o *SqllibOpener) Open(driverName, dataSourceName string) (SqlLibBackender, error) {
	return sql.Open(driverName, dataSourceName)
}

type SqlQuery struct {
	query string
	args  []interface{}
}

func NewSqlQuery(query string, args ...interface{}) *SqlQuery {
	return &SqlQuery{query: query, args: args}
}

type SqllibBackend struct {
	db SqlLibBackender
	Backender
}

type SqlLibBackender interface {
	Ping() error
	Close() error
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

func NewSqllibOneDB(driverName, connectionString string) (OneDBer, error) {
	conn, err := newSqllibBackend(driverName, connectionString)
	if err != nil {
		return nil, err
	}
	return NewBackendConverter(conn), nil
}

func newSqllibBackend(driverName, connectionString string) (Backender, error) {
	sqlDb, err := sqlOpen.Open(driverName, connectionString)
	if err != nil {
		return nil, err
	}
	err = sqlDb.Ping()
	if err != nil {
		return nil, err
	}
	return &SqllibBackend{db: sqlDb}, nil
}

func (b *SqllibBackend) Close() error {
	return b.db.Close()
}

func (b *SqllibBackend) Query(query interface{}) (RowsScanner, error) {
	q, ok := query.(*SqlQuery)
	if !ok {
		return nil, ErrInvalidQueryType
	}
	return b.db.Query(q.query, q.args...)
}

func (b *SqllibBackend) QueryRow(query interface{}) Scanner {
	q, ok := query.(*SqlQuery)
	if !ok {
		return &MockRowScanner{ScanErr: ErrInvalidQueryType}
	}
	return b.db.QueryRow(q.query, q.args...)
}
