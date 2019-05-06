package sql

import (
	sqllib "database/sql"

	"github.com/EndFirstCorp/onedb"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
)

type openDatabaseFunc func(driverName, dataSourceName string) (sqlLibBackender, error)

var openDatabase openDatabaseFunc = sqllibOpen

func sqllibOpen(driverName, dataSourceName string) (sqlLibBackender, error) {
	return sqllib.Open(driverName, dataSourceName)
}

type sqllibBackend struct {
	db sqlLibBackender
	onedb.Backender
}

// SQLer is the interface containing the capability available for a database/sql database
type SQLer interface {
	onedb.DBer
}

type sqlLibBackender interface {
	Ping() error
	Close() error
	Exec(query string, args ...interface{}) (sqllib.Result, error)
	Query(query string, args ...interface{}) (*sqllib.Rows, error)
}

// NewSqllib creates an instance of a database/sql database
func NewSqllib(driverName, connectionString string) (SQLer, error) {
	sqlDb, err := openDatabase(driverName, connectionString)
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
