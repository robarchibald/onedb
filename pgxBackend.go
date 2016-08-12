package onedb

import (
	"gopkg.in/jackc/pgx.v2"
)

var pgxOpen ConnPoolNewer = &PgxConnPooler{}

type ConnPoolNewer interface {
	NewConnPool(config pgx.ConnPoolConfig) (p PgxBackender, err error)
}

type PgxConnPooler struct{}

func (c *PgxConnPooler) NewConnPool(config pgx.ConnPoolConfig) (p PgxBackender, err error) {
	return pgx.NewConnPool(config)
}

type PgxBackend struct {
	db PgxBackender
	Backender
}

type PgxBackender interface {
	Close()
	Exec(query string, args ...interface{}) (pgx.CommandTag, error)
	Query(query string, args ...interface{}) (*pgx.Rows, error)
	QueryRow(query string, args ...interface{}) *pgx.Row
}

func NewPgxOneDB(server string, port uint16, username string, password string, database string) (OneDBer, error) {
	conn, err := newPgxBackend(server, port, username, password, database)
	if err != nil {
		return nil, err
	}
	return NewBackendConverter(conn), nil
}

func newPgxBackend(server string, port uint16, username string, password string, database string) (Backender, error) {
	connConfig := pgx.ConnConfig{Host: server, Port: port, User: username, Password: password, Database: database}
	poolConfig := pgx.ConnPoolConfig{ConnConfig: connConfig, MaxConnections: 10}
	pgxDb, err := pgxOpen.NewConnPool(poolConfig)
	if err != nil {
		return nil, err
	}

	return &PgxBackend{db: pgxDb}, nil
}

func (b *PgxBackend) Close() error {
	b.db.Close()
	return nil
}

func (b *PgxBackend) Query(query interface{}) (RowsScanner, error) {
	q, ok := query.(*SqlQuery)
	if !ok {
		return nil, ErrInvalidQueryType
	}
	rows, _ := b.db.Query(q.query, q.args...)
	return &PgxRows{rows: rows}, rows.Err()
}

func (b *PgxBackend) QueryRow(query interface{}) Scanner {
	q, ok := query.(*SqlQuery)
	if !ok {
		return &ErrorScanner{ErrInvalidQueryType}
	}
	return b.db.QueryRow(q.query, q.args...)
}

type PgxRows struct {
	rows PgxRower
	RowsScanner
}

type PgxRower interface {
	Close()
	Err() error
	Next() bool
	FieldDescriptions() []pgx.FieldDescription
	Values() ([]interface{}, error)
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
