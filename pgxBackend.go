package onedb

import (
	"github.com/pkg/errors"
	"gopkg.in/jackc/pgx.v2"
	"math"
	"net"
	"strings"
	"time"
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
	backender
}

type pgxBackender interface {
	Close()
	Exec(query string, args ...interface{}) (pgx.CommandTag, error)
	Query(query string, args ...interface{}) (*pgx.Rows, error)
	QueryRow(query string, args ...interface{}) *pgx.Row
}

func NewPgx(server string, port uint16, username string, password string, database string) (DBer, error) {
	connConfig := pgx.ConnConfig{Host: server, Port: port, User: username, Password: password, Database: database, Dial: dialHelper.Dial}
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

func (b *pgxBackend) QueryRow(query interface{}) scanner {
	q, ok := query.(*SqlQuery)
	if !ok {
		return &errorScanner{}
	}
	row := b.db.QueryRow(q.query, q.args...)
	return &pgxRow{row: row}
}

func (b *pgxBackend) Query(query interface{}) (rowsScanner, error) {
	q, ok := query.(*SqlQuery)
	if !ok {
		return nil, errInvalidSqlQueryType
	}
	rows, err := b.db.Query(q.query, q.args...)
	if (err == pgx.ErrDeadConn || err != nil && strings.HasSuffix(err.Error(), "connection reset by peer")) && b.reconnect() {
		return b.Query(query)
	} else if err != nil {
		return nil, err
	}
	return &pgxRows{rows: rows}, rows.Err()
}

func (b *pgxBackend) Execute(command interface{}) error {
	c, ok := command.(*SqlQuery)
	if !ok {
		return errInvalidSqlQueryType
	}
	_, err := b.db.Exec(c.query, c.args...)
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
