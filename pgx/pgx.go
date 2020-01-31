package pgx

import (
	"io"

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
	return newPgx(&pgx.ConnConfig{Host: server, Port: port, User: username, Password: password, Database: database, Dial: onedb.DialTCP})
}

func newPgx(connConfig *pgx.ConnConfig) (PGXer, error) {
	poolConfig := pgx.ConnPoolConfig{ConnConfig: *connConfig, MaxConnections: 10}
	pgxDb, err := pgx.NewConnPool(poolConfig)
	if err != nil {
		return nil, err
	}

	return &pgxBackend{db: &pgxWithReconnect{db: pgxDb}}, nil
}

func (b *pgxBackend) Begin() (Txer, error) {
	return b.db.Begin()
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

func (b *pgxBackend) QueryValues(query *onedb.Query, result ...interface{}) error {
	return onedb.QueryValues(b, query, result...)
}

func (b *pgxBackend) QueryJSON(query string, args ...interface{}) (string, error) {
	return onedb.QueryJSON(b, query, args...)
}

func (b *pgxBackend) QueryJSONRow(query string, args ...interface{}) (string, error) {
	return onedb.QueryJSONRow(b, query, args...)
}

func (b *pgxBackend) QueryStruct(result interface{}, query string, args ...interface{}) error {
	return onedb.QueryStruct(b, result, query, args...)
}

func (b *pgxBackend) QueryStructRow(result interface{}, query string, args ...interface{}) error {
	return onedb.QueryStructRow(b, result, query, args...)
}

func (b *pgxBackend) QueryWriteCSV(w io.Writer, options onedb.CSVOptions, query string, args ...interface{}) error {
	return onedb.QueryWriteCSV(w, options, b, query, args...)
}

type pgxTx struct {
	tx *pgx.Tx
	Txer
}

type Txer interface {
	Commit() error
	Conn() *pgx.Conn
	Rollback() error
	Status() int8
	PGXQuerier
}

type PGXQuerier interface {
	querier
	onedb.DBer
}

func (t *pgxTx) Commit() error {
	return t.tx.Commit()
}

func (t *pgxTx) Conn() *pgx.Conn {
	return t.tx.Conn()
}

func (t *pgxTx) Rollback() error {
	return t.tx.Rollback()
}

func (t *pgxTx) Status() int8 {
	return t.tx.Status()
}

func (t *pgxTx) CopyFrom(tableName Identifier, columnNames []string, rows CopyFromSource) (int, error) {
	return t.tx.CopyFrom(pgx.Identifier(tableName), columnNames, rows)
}

func (t *pgxTx) QueryRow(query string, args ...interface{}) onedb.Scanner {
	return t.tx.QueryRow(query, args...)
}

func (t *pgxTx) Query(query string, args ...interface{}) (onedb.RowsScanner, error) {
	rows, err := t.tx.Query(query, args...)
	if err != nil {
		return nil, err
	}
	return &pgxRows{rows: rows}, rows.Err()
}

func (t *pgxTx) Exec(query string, args ...interface{}) (CommandTag, error) {
	tag, err := t.tx.Exec(query, args...)
	return CommandTag(tag), err
}

func (t *pgxTx) QueryValues(query *onedb.Query, result ...interface{}) error {
	return onedb.QueryValues(t, query, result...)
}

func (t *pgxTx) QueryJSON(query string, args ...interface{}) (string, error) {
	return onedb.QueryJSON(t, query, args...)
}

func (t *pgxTx) QueryJSONRow(query string, args ...interface{}) (string, error) {
	return onedb.QueryJSONRow(t, query, args...)
}

func (t *pgxTx) QueryStruct(result interface{}, query string, args ...interface{}) error {
	return onedb.QueryStruct(t, result, query, args...)
}

func (t *pgxTx) QueryStructRow(result interface{}, query string, args ...interface{}) error {
	return onedb.QueryStructRow(t, result, query, args...)
}

func (t *pgxTx) QueryWriteCSV(w io.Writer, options onedb.CSVOptions, query string, args ...interface{}) error {
	return onedb.QueryWriteCSV(w, options, t, query, args...)
}
