package pgx

import (
	"io"
	"strings"

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

// CreateFTS creates a query optimized for full text search against a psql index
func CreateFTS(query string, lenToWildcard int) string {
	var fts strings.Builder
	terms := getTerms(query)
	for i, term := range terms {
		fts.WriteString(term)
		if len(term) >= lenToWildcard || len(terms) > 1 {
			fts.WriteString(":*") // starts with filter
		}
		if len(terms) != i+1 {
			fts.WriteString(" & ")
		}
	}
	return fts.String()
}

func getTerms(query string) []string {
	rawTerms := strings.Split(strings.Trim(query, " "), " ")
	terms := []string{}
	for i := 0; i < len(rawTerms); i++ {
		if rawTerms[i] != "" {
			terms = append(terms, rawTerms[i])
		}
	}
	return terms
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
