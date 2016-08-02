package testableDb

import (
	"database/sql"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
)

func newSqllibBackend(driverName, connectionString string) (BackendConnecter, error) {
	sqlDb, err := sql.Open(driverName, connectionString)
	if err != nil {
		return nil, err
	}
	err = sqlDb.Ping()
	if err != nil {
		return nil, err
	}
	return &SqllibBackend{sqlDb: sqlDb}, nil
}

type SqllibBackend struct {
	sqlDb *sql.DB
	BackendConnecter
}

func (w *SqllibBackend) Close() error {
	return w.sqlDb.Close()
}

func (w *SqllibBackend) Query(query string, args ...interface{}) (RowsScanner, error) {
	return w.sqlDb.Query(query, args...)
}

func (w *SqllibBackend) QueryRow(query string, args ...interface{}) RowScanner {
	return w.sqlDb.QueryRow(query, args...)
}
