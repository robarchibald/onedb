package onedb

import (
	"gopkg.in/jackc/pgx.v2"
)

var pgxCreate pgxCreator = &pgxRealCreator{}

type pgxCreator interface {
	newConnPool(config pgx.ConnPoolConfig) (p pgxBackender, err error)
}

type pgxRealCreator struct{}

func (c *pgxRealCreator) newConnPool(config pgx.ConnPoolConfig) (p pgxBackender, err error) {
	return pgx.NewConnPool(config)
}

type pgxBackend struct {
	db pgxBackender
	backender
}

type pgxBackender interface {
	Close()
	Exec(query string, args ...interface{}) (pgx.CommandTag, error)
	Query(query string, args ...interface{}) (*pgx.Rows, error)
	QueryRow(query string, args ...interface{}) *pgx.Row
}

func NewPgx(server string, port uint16, username string, password string, database string) (DBer, error) {
	connConfig := pgx.ConnConfig{Host: server, Port: port, User: username, Password: password, Database: database}
	poolConfig := pgx.ConnPoolConfig{ConnConfig: connConfig, MaxConnections: 10}
	pgxDb, err := pgxCreate.newConnPool(poolConfig)
	if err != nil {
		return nil, err
	}

	return newBackendConverter(&pgxBackend{db: pgxDb}), nil
}

func (b *pgxBackend) Close() error {
	b.db.Close()
	return nil
}

func (b *pgxBackend) Query(query interface{}) (rowsScanner, error) {
	q, ok := query.(*SqlQuery)
	if !ok {
		return nil, errInvalidQueryType
	}
	rows, _ := b.db.Query(q.query, q.args...)
	return &pgxRows{rows: rows}, rows.Err()
}

func (b *pgxBackend) QueryRow(query interface{}) scanner {
	q, ok := query.(*SqlQuery)
	if !ok {
		return &errorScanner{errInvalidQueryType}
	}
	return b.db.QueryRow(q.query, q.args...)
}

type pgxRows struct {
	rows pgxRower
	rowsScanner
}

type pgxRower interface {
	Close()
	Err() error
	Next() bool
	FieldDescriptions() []pgx.FieldDescription
	Values() ([]interface{}, error)
}

func (r *pgxRows) Columns() ([]string, error) {
	fields := r.rows.FieldDescriptions()
	columns := make([]string, len(fields))
	for i, field := range fields {
		columns[i] = field.Name
	}
	return columns, nil
}

func (r *pgxRows) Next() bool {
	return r.rows.Next()
}

func (r *pgxRows) Close() error {
	r.rows.Close()
	return nil
}

func (r *pgxRows) Scan(dest ...interface{}) error {
	vals, err := r.rows.Values()
	if err != nil {
		return err
	}
	for i, item := range dest {
		*(item.(*interface{})) = vals[i]
	}
	return nil
}

func (r *pgxRows) Err() error {
	return r.rows.Err()
}
