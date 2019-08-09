package sql

import (
	sqllib "database/sql"
	"io"

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
	QueryRow(query string, args ...interface{}) *sqllib.Row
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
	return &sqllibBackend{db: sqlDb}, nil
}

func (b *sqllibBackend) Close() error {
	return b.db.Close()
}

func (b *sqllibBackend) Query(query string, args ...interface{}) (onedb.RowsScanner, error) {
	return b.db.Query(query, args...)
}

func (b *sqllibBackend) QueryRow(query string, args ...interface{}) onedb.Scanner {
	return b.db.QueryRow(query, args...)
}

func (b *sqllibBackend) Exec(command string, args ...interface{}) error {
	_, err := b.db.Exec(command, args...)
	return err
}

func (b *sqllibBackend) QueryValues(query *onedb.Query, result ...interface{}) error {
	return onedb.QueryValues(b, query, result)
}

func (b *sqllibBackend) QueryJSON(query string, args ...interface{}) (string, error) {
	return onedb.QueryJSON(b, query, args...)
}

func (b *sqllibBackend) QueryJSONRow(query string, args ...interface{}) (string, error) {
	return onedb.QueryJSONRow(b, query, args...)
}

func (b *sqllibBackend) QueryStruct(result interface{}, query string, args ...interface{}) error {
	return onedb.QueryStruct(b, result, query, args...)
}

func (b *sqllibBackend) QueryStructRow(result interface{}, query string, args ...interface{}) error {
	return onedb.QueryStructRow(b, result, query, args...)
}

func (b *sqllibBackend) QueryWriteCSV(w io.Writer, options onedb.CSVOptions, query string, args ...interface{}) error {
	return onedb.QueryWriteCSV(w, options, b, query, args...)
}
