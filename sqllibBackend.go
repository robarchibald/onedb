package onedb

import (
	"database/sql"
	"errors"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
)

var ErrInvalidSqlQueryType = errors.New("Invalid query. Must be of type *SqlQuery")
var sqllibCreate sqllibCreator = &sqllibRealCreator{}

type sqllibCreator interface {
	Open(driverName, dataSourceName string) (sqlLibBackender, error)
}

type sqllibRealCreator struct{}

func (o *sqllibRealCreator) Open(driverName, dataSourceName string) (sqlLibBackender, error) {
	return sql.Open(driverName, dataSourceName)
}

type SqlQuery struct {
	Query string
	Args  []interface{}
}

func NewSqlQuery(query string, args ...interface{}) *SqlQuery {
	return &SqlQuery{Query: query, Args: args}
}

type sqllibBackend struct {
	db sqlLibBackender
	backender
}

//move me
type backender interface {
	Close() error
	Execute(query interface{}) error
	Query(query interface{}) (rowsScanner, error)
	QueryRow(query interface{}) scanner
}

type sqlLibBackender interface {
	Ping() error
	Close() error
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
}

func NewSqllib(driverName, connectionString string) (DBer, error) {
	sqlDb, err := sqllibCreate.Open(driverName, connectionString)
	if err != nil {
		return nil, err
	}
	err = sqlDb.Ping()
	if err != nil {
		return nil, err
	}
	return newBackendConverter(&sqllibBackend{db: sqlDb}), nil
}

type backendConverter struct {
	backend backender
	DBer
}

func newBackendConverter(backend backender) DBer {
	return &backendConverter{backend: backend}
}

func (b *sqllibBackend) Close() error {
	return b.db.Close()
}

func (b *sqllibBackend) Query(query interface{}) (rowsScanner, error) {
	q, ok := query.(*SqlQuery)
	if !ok {
		return nil, ErrInvalidSqlQueryType
	}
	return b.db.Query(q.Query, q.Args...)
}

func (b *sqllibBackend) Execute(command interface{}) error {
	c, ok := command.(*SqlQuery)
	if !ok {
		return ErrInvalidSqlQueryType
	}
	_, err := b.db.Exec(c.Query, c.Args...)
	return err
}
