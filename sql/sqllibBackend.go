package sql

import (
	sqllib "database/sql"

	"github.com/EndFirstCorp/onedb"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
)

var sqllibCreate sqllibCreator = &sqllibRealCreator{}

type sqllibCreator interface {
	Open(driverName, dataSourceName string) (sqlLibBackender, error)
}

type sqllibRealCreator struct{}

func (o *sqllibRealCreator) Open(driverName, dataSourceName string) (sqlLibBackender, error) {
	return sqllib.Open(driverName, dataSourceName)
}

type sqllibBackend struct {
	db sqlLibBackender
	backender
}

//move me
type backender interface {
	Close() error
	Execute(query interface{}) error
	Query(query interface{}) (onedb.RowsScanner, error)
	QueryRow(query interface{}) onedb.Scanner
}

type sqlLibBackender interface {
	Ping() error
	Close() error
	Exec(query string, args ...interface{}) (sqllib.Result, error)
	Query(query string, args ...interface{}) (*sqllib.Rows, error)
}

func NewSqllib(driverName, connectionString string) (onedb.DBer, error) {
	sqlDb, err := sqllibCreate.Open(driverName, connectionString)
	if err != nil {
		return nil, err
	}
	err = sqlDb.Ping()
	if err != nil {
		return nil, err
	}
	// return NewBackendConverter(&sqllibBackend{db: sqlDb}), nil
	return nil, nil
	// need some help deciding on this one, if we move newbackendconverter, and thus backendconverter, into the main repo, we will have to move eery single method (basically the whole file including my custom PGX functionality) from backendconverter to the main repo
}

func (b *sqllibBackend) Close() error {
	return b.db.Close()
}

func (b *sqllibBackend) Query(query string, args ...interface{}) (onedb.RowsScanner, error) {
	return b.db.Query(query, args...)
}

func (b *sqllibBackend) Execute(command string, args ...interface{}) error {
	_, err := b.db.Exec(command, args...)
	return err
}
