package pgxo

import (
	"fmt"
	"math"
	"net"
	"strings"
	"time"

	"github.com/EndFirstCorp/onedb"
	"github.com/pkg/errors"
	pgx "gopkg.in/jackc/pgx.v2"
)

var pgxCreate pgxCreator = &pgxRealCreator{}
var dialHelper dialer = &realDialer{}

type pgxCreator interface {
	newConnPool(config pgx.ConnPoolConfig) (p pgxBackender, err error)
}

type pgxRealCreator struct{}

func (c *pgxRealCreator) newConnPool(config pgx.ConnPoolConfig) (p pgxBackender, err error) {
	return pgx.NewConnPool(config)
}

type dialer interface {
	Dial(network, addr string) (net.Conn, error)
}

type realDialer struct{}

func (d *realDialer) Dial(network, addr string) (net.Conn, error) {
	tcpAddr, err := net.ResolveTCPAddr(network, addr)
	if err != nil {
		return nil, err
	}
	tc, err := net.DialTCP(network, nil, tcpAddr)
	if err != nil {
		return nil, err
	}
	if err := tc.SetKeepAlive(true); err != nil {
		return nil, err
	}
	if err := tc.SetKeepAlivePeriod(2 * time.Minute); err != nil {
		return nil, err
	}
	return tc, nil
}

type pgxBackend struct {
	db         pgxBackender
	lastRetry  time.Time
	retryCount int
	pgxBackender
}

type pgxBackender interface {
	Close()
	Exec(query string, args ...interface{}) (pgx.CommandTag, error)
	Query(query string, args ...interface{}) (*pgx.Rows, error)
	QueryRow(query string, args ...interface{}) *pgx.Row
	CopyFrom(tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int, error)
}

// NewPgxFromURI returns a PGX DBer instance from a connection URI
func NewPgxFromURI(uri string) (DBer, error) {
	connConfig, err := pgx.ParseURI(uri)
	if err != nil {
		return nil, err
	}
	return newPgx(&connConfig)
}

// NewPgx returns a PGX DBer instance from a set of parameters
func NewPgx(server string, port uint16, username string, password string, database string) (DBer, error) {
	return newPgx(&pgx.ConnConfig{Host: server, Port: port, User: username, Password: password, Database: database, Dial: dialHelper.Dial})
}

func newPgx(connConfig *pgx.ConnConfig) (DBer, error) {
	poolConfig := pgx.ConnPoolConfig{ConnConfig: *connConfig, MaxConnections: 10}
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

func (b *pgxBackend) CopyFrom(tableName string, columnNames []string, rows [][]interface{}) (int, error) {
	fmt.Println("copyfrom before, tableName:", tableName, "rows:", rows, "columnNames:", columnNames)
	// fmt.Println("copyfrom before")
	// rows = [][]interface{}{
	// 	{"5cc9f8281dd9a192d51db9c2", "5a62a536d030123da5af35d1", "2018-01-23 17:03:31.921", "HEW004724093", "HEW004724093", "1"},
	// 	{"5a62a536d030123da5af35d1", "5cc9f8281dd9a192d51db9c2", "2019-05-01 12:48:56.717825", "HEW004724093", "HEW004724093", "1"},
	// }
	// tableName = "duplicates"
	fmt.Println("b", b)
	fmt.Println("CopyFromRows:", pgx.CopyFromRows(rows))
	copyCount, err := b.db.CopyFrom(
		pgx.Identifier{tableName},
		// pgx.Identifier{"public", "duplicates"},
		columnNames,
		// []string{"claimid", "matchid", "matchdate", "matchclaimnumber", "matchreferencenumber", "matchfraction"},
		pgx.CopyFromRows(rows),
	)
	fmt.Println("copyfrom after")
	return copyCount, err
}

func (b *pgxBackend) QueryRow(query interface{}) scanner {
	q, ok := query.(*onedb.SqlQuery)
	if !ok {
		return &onedb.ErrorScanner{}
	}
	row := b.db.QueryRow(q.Query, q.Args...)
	return &pgxRow{row: row}
}

func (b *pgxBackend) Query(query interface{}) (rowsScanner, error) {
	q, ok := query.(*onedb.SqlQuery)
	if !ok {
		return nil, onedb.ErrInvalidSqlQueryType
	}
	rows, err := b.db.Query(q.Query, q.Args...)
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
