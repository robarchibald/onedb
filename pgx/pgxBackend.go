package pgx

import (
	"math"
	"strings"
	"time"

	"github.com/EndFirstCorp/onedb"
	"github.com/pkg/errors"
	pgx "gopkg.in/jackc/pgx.v2"
)

// Identifier a PostgreSQL identifier or name. Identifiers can be composed of
// multiple parts such as ["schema", "table"] or ["table", "column"].
type Identifier []string

// CopyFromSource is the interface used by *Conn.CopyFrom as the source for copy data.
type CopyFromSource interface {
	// Next returns true if there is another row and makes the next row data
	// available to Values(). When there are no more rows available or an error
	// has occurred it returns false.
	Next() bool

	// Values returns the values for the current row.
	Values() ([]interface{}, error)

	// Err returns any error that has been encountered by the CopyFromSource. If
	// this is not nil *Conn.CopyFrom will abort the copy.
	Err() error
}

type pgxBackend struct {
	db         *pgx.ConnPool
	lastRetry  time.Time
	retryCount int
	PGXer
}

type rowsScanner interface {
	Columns() ([]string, error)
	Next() bool
	Close() error
	Err() error
	scanner
}

type scanner interface {
	Scan(dest ...interface{}) error
}

type PGXer interface {
	Close()
	Exec(query string, args ...interface{}) (pgx.CommandTag, error)
	Query(query string, args ...interface{}) (rowsScanner, error)
	QueryRow(query string, args ...interface{}) scanner
	CopyFrom(tableName Identifier, columnNames []string, rowSrc CopyFromSource) (int, error)

	QueryValues(query onedb.SqlQuery, result ...interface{}) error
	QueryJSON(query string, args ...interface{}) (string, error)
	QueryJSONRow(query string, args ...interface{}) (string, error)
	QueryStruct(result interface{}, query string, args ...interface{}) error
	QueryStructRow(result interface{}, query string, args ...interface{}) error
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

	return &pgxBackend{db: pgxDb}, nil
}

func (b *pgxBackend) Close() {
	b.db.Close()
}

func (b *pgxBackend) QueryValues(query onedb.SqlQuery, result ...interface{}) error {
	row := b.QueryRow(query.Query, query.Args)
	return row.Scan(result...)
}

func (b *pgxBackend) QueryJSON(query string, args ...interface{}) (string, error) {
	rows, err := b.Query(query, args)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	return onedb.GetJSON(rows)
}

func (b *pgxBackend) QueryJSONRow(query string, args ...interface{}) (string, error) {
	rows, err := b.Query(query, args)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	return onedb.GetJSONRow(rows)
}

func (b *pgxBackend) QueryStruct(result interface{}, query string, args ...interface{}) error {
	rows, err := b.Query(query, args)
	if err != nil {
		return err
	}
	defer rows.Close()

	return onedb.GetStruct(rows, result)
}

func (b *pgxBackend) QueryStructRow(result interface{}, query string, args ...interface{}) error {
	rows, err := b.Query(query, args)
	if err != nil {
		return err
	}
	defer rows.Close()

	return onedb.GetStructRow(rows, result)
}

func (b *pgxBackend) CopyFrom(tableName Identifier, columnNames []string, rows CopyFromSource) (int, error) {
	return b.db.CopyFrom(pgx.Identifier(tableName), columnNames, rows)
}

func (b *pgxBackend) QueryRow(query string, args ...interface{}) scanner {
	return b.db.QueryRow(query, args...)
}

func (b *pgxBackend) Query(query string, args ...interface{}) (rowsScanner, error) {
	rows, err := b.db.Query(query, args...)
	if (err == pgx.ErrDeadConn || err != nil && strings.HasSuffix(err.Error(), "connection reset by peer")) && b.reconnect() {
		return b.Query(query)
	} else if err != nil {
		return nil, err
	}
	return &pgxRows{rows: rows}, rows.Err()
}

func (b *pgxBackend) Execute(command interface{}) error {
	c, ok := command.(*onedb.SqlQuery)
	if !ok {
		return onedb.ErrInvalidSqlQueryType
	}
	_, err := b.db.Exec(c.Query, c.Args...)
	if (err == pgx.ErrDeadConn || err != nil && strings.HasSuffix(err.Error(), "connection reset by peer")) && b.reconnect() {
		return b.Execute(command)
	}
	return err
}

func (b *pgxBackend) ping() error {
	var val int
	if err := b.db.QueryRow("select 1 + 1").Scan(&val); err != nil {
		return err
	}
	if val != 2 {
		return errors.New("Failed ping test")
	}
	return nil
}

func (b *pgxBackend) reconnect() bool {
	ms := time.Millisecond * time.Duration(math.Pow10(b.retryCount)) // retry every 10^lastRetry milliseconds
	if time.Since(b.lastRetry) > ms {
		b.lastRetry = time.Now()
		err := b.ping()
		if err == nil {
			b.retryCount = 0
			return true
		} else if b.retryCount < 4 { // max retry time is 10 seconds
			b.retryCount++
		}
	}
	return false
}

type pgxRow struct {
	row scanner
}

func (r *pgxRow) Scan(dest ...interface{}) error {
	return r.row.Scan(dest...)
}

type pgxRows struct {
	rows pgxRower
	rowsScanner
	scanner
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
