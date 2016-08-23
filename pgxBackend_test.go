package onedb

import (
	"errors"
	"testing"

	"gopkg.in/jackc/pgx.v2"
)

func TestNewPgx(t *testing.T) {
	pgxCreate = &pgxMockCreator{}
	_, err := NewPgx("localhost", 5432, "user", "password", "database")
	if err != nil {
		t.Error("expected success")
	}

	pgxCreate = &pgxMockCreator{Err: errors.New("fail")}
	_, err = NewPgx("localhost", 5432, "user", "password", "database")
	if err == nil {
		t.Error("expected fail")
	}
}

func TestNewPgxOneDBRealConnection(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	pgxCreate = &pgxRealCreator{}
	_, err := NewPgx("localhost", 5432, "user", "password", "database")
	if err != nil {
		t.Error("expected connection success", err)
	}
}

func TestPgxClose(t *testing.T) {
	c := newMockPgx()
	d := &pgxBackend{db: c}
	d.Close()
	if len(c.MethodsCalled) != 1 || len(c.MethodsCalled["Close"]) != 1 {
		t.Error("expected close method to be called on backend")
	}
}

func TestPgxQuery(t *testing.T) {
	c := newMockPgx()
	d := &pgxBackend{db: c}
	_, err := d.Query("bogus")
	if err == nil {
		t.Error("expected error")
	}

	d.Query(NewSqlQuery("query", "arg1", "arg2"))
	queries := c.MethodsCalled["Query"]
	if len(c.MethodsCalled) != 1 || len(queries) != 1 ||
		queries[0].(*SqlQuery).query != "query" ||
		queries[0].(*SqlQuery).args[0] != "arg1" ||
		queries[0].(*SqlQuery).args[1] != "arg2" {
		t.Error("expected query method to be called on backend")
	}
}

func TestPgxRowsColumns(t *testing.T) {
	m := newMockPgxRows()
	r := &pgxRows{rows: m}
	c, _ := r.Columns()
	if len(m.MethodsCalled["FieldDescriptions"]) != 1 || len(c) != 2 || c[0] != "F1" || c[1] != "F2" {
		t.Error("expected FieldDescriptions method to be called")
	}
}

func TestPgxRowsNext(t *testing.T) {
	m := newMockPgxRows()
	r := &pgxRows{rows: m}

	if r.Next() || len(m.MethodsCalled["Next"]) != 1 {
		t.Error("expected Next method to be called")
	}
}

func TestPgxRowsClose(t *testing.T) {
	m := newMockPgxRows()
	r := &pgxRows{rows: m}
	r.Close()
	if len(m.MethodsCalled["Close"]) != 1 {
		t.Error("expected Close method to be called")
	}
}

func TestPgxRowsScan(t *testing.T) {
	m := newMockPgxRows()
	r := &pgxRows{rows: m}
	var id interface{}
	var name interface{}
	r.Scan(&id, &name)
	if len(m.MethodsCalled["Values"]) != 1 || id != 1234 || name != "hello" {
		t.Error("expected Values method to be called", id, name)
	}

	m.ValuesErr = errors.New("fail")
	err := r.Scan(&id, &name)
	if err == nil {
		t.Error("expected error")
	}
}

func TestPgxRowsErr(t *testing.T) {
	m := newMockPgxRows()
	r := &pgxRows{rows: m}
	r.Err()
	if len(m.MethodsCalled["Err"]) != 1 {
		t.Error("expected Err method to be called")
	}
}

/***************************** MOCKS ****************************/
type pgxMockCreator struct {
	connector pgxBackender
	Err       error
}

func (c *pgxMockCreator) newConnPool(config pgx.ConnPoolConfig) (p pgxBackender, err error) {
	if c.connector == nil {
		c.connector = newMockPgx()
	}
	return c.connector, c.Err
}

type mockPgx struct {
	MethodsCalled map[string][]interface{}
}

func newMockPgx() *mockPgx {
	return &mockPgx{MethodsCalled: make(map[string][]interface{})}
}

func (c *mockPgx) Close() {
	c.MethodsCalled["Close"] = append(c.MethodsCalled["Close"], nil)
}
func (c *mockPgx) Exec(query string, args ...interface{}) (pgx.CommandTag, error) {
	c.MethodsCalled["Exec"] = append(c.MethodsCalled["Exec"], NewSqlQuery(query, args...))
	return "tag", nil
}
func (c *mockPgx) Query(query string, args ...interface{}) (*pgx.Rows, error) {
	c.MethodsCalled["Query"] = append(c.MethodsCalled["Query"], NewSqlQuery(query, args...))
	return &pgx.Rows{}, nil
}
func (c *mockPgx) QueryRow(query string, args ...interface{}) *pgx.Row {
	c.MethodsCalled["QueryRow"] = append(c.MethodsCalled["QueryRow"], NewSqlQuery(query, args...))
	return nil
}

type mockPgxRows struct {
	MethodsCalled map[string][]interface{}
	ValuesErr     error
}

func newMockPgxRows() *mockPgxRows {
	return &mockPgxRows{MethodsCalled: make(map[string][]interface{})}
}

func (r *mockPgxRows) Close() {
	r.MethodsCalled["Close"] = append(r.MethodsCalled["Close"], nil)
}
func (r *mockPgxRows) Err() error {
	r.MethodsCalled["Err"] = append(r.MethodsCalled["Err"], nil)
	return nil
}
func (r *mockPgxRows) Next() bool {
	r.MethodsCalled["Next"] = append(r.MethodsCalled["Next"], nil)
	return false
}
func (r *mockPgxRows) FieldDescriptions() []pgx.FieldDescription {
	r.MethodsCalled["FieldDescriptions"] = append(r.MethodsCalled["FieldDescriptions"], nil)
	return []pgx.FieldDescription{pgx.FieldDescription{Name: "F1"}, pgx.FieldDescription{Name: "F2"}}
}
func (r *mockPgxRows) Values() ([]interface{}, error) {
	r.MethodsCalled["Values"] = append(r.MethodsCalled["Values"], nil)
	return []interface{}{1234, "hello"}, r.ValuesErr
}
