package testableDb

import (
	"gopkg.in/jackc/pgx.v2"
)

func newPgxBackend(server string, port uint16, username string, password string, database string) (BackendConnecter, error) {
	connConfig := pgx.ConnConfig{Host: server, Port: port, User: username, Password: password, Database: database}
	poolConfig := pgx.ConnPoolConfig{ConnConfig: connConfig, MaxConnections: 10}
	pgxDb, err := pgx.NewConnPool(poolConfig)
	if err != nil {
		return nil, err
	}

	return &PgxBackend{pgxDb: pgxDb}, nil
}

type PgxBackend struct {
	pgxDb *pgx.ConnPool
	BackendConnecter
}

func (w *PgxBackend) Close() error {
	w.pgxDb.Close()
	return nil
}

func (w *PgxBackend) Query(query string, args ...interface{}) (RowsScanner, error) {
	rows, _ := w.pgxDb.Query(query, args...)
	return &PgxRows{rows: rows}, rows.Err()
}

func (w *PgxBackend) QueryRow(query string, args ...interface{}) RowScanner {
	return w.pgxDb.QueryRow(query, args...)
}

type PgxRows struct {
	rows *pgx.Rows
	RowsScanner
}

func (r *PgxRows) Columns() ([]string, error) {
	fields := r.rows.FieldDescriptions()
	columns := make([]string, len(fields))
	for i, field := range fields {
		columns[i] = field.Name
	}
	return columns, nil
}

func (r *PgxRows) Next() bool {
	return r.rows.Next()
}

func (r *PgxRows) Close() error {
	r.rows.Close()
	return nil
}

func (r *PgxRows) Scan(dest ...interface{}) error {
	vals, err := r.rows.Values()
	if err != nil {
		return err
	}
	for i, item := range dest {
		*(item.(*interface{})) = vals[i]
	}
	return nil
}

func (r *PgxRows) Err() error {
	return r.rows.Err()
}
