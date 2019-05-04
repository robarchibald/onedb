package pgx

import (
	"github.com/EndFirstCorp/onedb"
	pgx "gopkg.in/jackc/pgx.v2"
)

type pgxBackend struct {
	db pgxWrapper
	PGXer
}

type PGXer interface {
	pgxWrapper
	onedb.DBer
}

// NewPgxFromURI returns a PGX DBer instance from a connection URI
func NewPgxFromURI(uri string) (PGXer, error) {
	connConfig, err := pgx.ParseURI(uri)
	if err != nil {
		return nil, err
	}
	return newPgx(&connConfig)
}

// NewPgx returns a PGX DBer instance from a set of parameters
func NewPgx(server string, port uint16, username string, password string, database string) (PGXer, error) {
	return newPgx(&pgx.ConnConfig{Host: server, Port: port, User: username, Password: password, Database: database})
}

func newPgx(connConfig *pgx.ConnConfig) (PGXer, error) {
	poolConfig := pgx.ConnPoolConfig{ConnConfig: *connConfig, MaxConnections: 10}
	pgxDb, err := pgx.NewConnPool(poolConfig)
	if err != nil {
		return nil, err
	}

	return &pgxBackend{db: &pgxWithReconnect{db: pgxDb}}, nil
}

func (b *pgxBackend) Close() {
	b.db.Close()
}
func (b *pgxBackend) Exec(query string, args ...interface{}) (CommandTag, error) {
	return b.db.Exec(query, args...)
}
func (b *pgxBackend) Query(query string, args ...interface{}) (onedb.RowsScanner, error) {
	return b.db.Query(query, args...)
}
func (b *pgxBackend) QueryRow(query string, args ...interface{}) onedb.Scanner {
	return b.db.QueryRow(query, args...)
}
func (b *pgxBackend) CopyFrom(tableName Identifier, columnNames []string, rowSrc CopyFromSource) (int, error) {
	return b.db.CopyFrom(tableName, columnNames, rowSrc)
}

func (b *pgxBackend) QueryValues(query *onedb.SqlQuery, result ...interface{}) error {
	if query == nil {
		return onedb.ErrQueryIsNil
	}
	return onedb.QueryValues(b, query, result...)
}

func (b *pgxBackend) QueryJSON(query string, args ...interface{}) (string, error) {
	rows, err := b.Query(query, args...)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	return onedb.GetJSON(rows)
}

func (b *pgxBackend) QueryJSONRow(query string, args ...interface{}) (string, error) {
	rows, err := b.Query(query, args...)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	return onedb.GetJSONRow(rows)
}

func (b *pgxBackend) QueryStruct(result interface{}, query string, args ...interface{}) error {
	rows, err := b.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	return onedb.GetStruct(rows, result)
}

func (b *pgxBackend) QueryStructRow(result interface{}, query string, args ...interface{}) error {
	rows, err := b.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	return onedb.GetStructRow(rows, result)
}
