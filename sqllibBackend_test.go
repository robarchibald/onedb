package onedb

import (
	"database/sql"
	"errors"
	"testing"
)

const connectionString string = "server=localhost;user id=gotest;password=go;database=GoTest;encrypt=disable"

func TestNewSqlQuery(t *testing.T) {
	q := NewSqlQuery("query", "arg1", "arg2")
	if q == nil || q.query != "query" || len(q.args) != 2 || q.args[0] != "arg1" || q.args[1] != "arg2" {
		t.Error("expected success")
	}
}

func TestNewSqllibOneDB(t *testing.T) {
	sqllibCreate = &sqllibMockCreator{}
	_, err := NewSqllib("mssql", connectionString)
	if err != nil {
		t.Error("expected success")
	}

	sqllibCreate = &sqllibMockCreator{Err: errors.New("fail")}
	_, err = NewSqllib("mssql", connectionString)
	if err == nil {
		t.Error("expected error")
	}

	sqllibCreate = &sqllibMockCreator{conn: &mockSqllibBackend{PingErr: errors.New("fail")}}
	_, err = NewSqllib("mssql", connectionString)
	if err == nil {
		t.Error("expected fail on ping")
	}
}

func TestNewSqllibOneDBRealConnection(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	sqllibCreate = &sqllibRealCreator{}
	_, err := NewSqllib("mssql", connectionString)
	if err != nil {
		t.Error("expected connection success", err)
	}
}

func TestSqllibClose(t *testing.T) {
	c := newMockSqllibBackend()
	d := &sqllibBackend{db: c}
	d.Close()
	if len(c.MethodsCalled) != 1 || len(c.MethodsCalled["Close"]) != 1 {
		t.Error("expected close method to be called on backend")
	}
}

func TestSqllibQuery(t *testing.T) {
	c := newMockSqllibBackend()
	d := &sqllibBackend{db: c}
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

func TestSqllibExecute(t *testing.T) {
	c := newMockSqllibBackend()
	d := &sqllibBackend{db: c}
	err := d.Execute("bogus")
	if err == nil {
		t.Error("expected error")
	}

	d.Execute(NewSqlQuery("query", "arg1", "arg2"))
	queries := c.MethodsCalled["Exec"]
	if len(c.MethodsCalled) != 1 || len(queries) != 1 ||
		queries[0].(*SqlQuery).query != "query" ||
		queries[0].(*SqlQuery).args[0] != "arg1" ||
		queries[0].(*SqlQuery).args[1] != "arg2" {
		t.Error("expected query method to be called on backend")
	}
}

/***************************** MOCKS ****************************/
type sqllibMockCreator struct {
	conn sqlLibBackender
	Err  error
}

func (s *sqllibMockCreator) Open(driverName, dataSourceName string) (sqlLibBackender, error) {
	if s.conn == nil {
		s.conn = newMockSqllibBackend()
	}
	return s.conn, s.Err
}

type mockSqllibBackend struct {
	MethodsCalled map[string][]interface{}
	PingErr       error
}

func newMockSqllibBackend() *mockSqllibBackend {
	return &mockSqllibBackend{MethodsCalled: make(map[string][]interface{})}
}

func (c *mockSqllibBackend) Ping() error {
	return c.PingErr
}

func (c *mockSqllibBackend) Close() error {
	c.MethodsCalled["Close"] = append(c.MethodsCalled["Close"], nil)
	return nil
}
func (c *mockSqllibBackend) Exec(query string, args ...interface{}) (sql.Result, error) {
	c.MethodsCalled["Exec"] = append(c.MethodsCalled["Exec"], NewSqlQuery(query, args...))
	return nil, nil
}
func (c *mockSqllibBackend) Query(query string, args ...interface{}) (*sql.Rows, error) {
	c.MethodsCalled["Query"] = append(c.MethodsCalled["Query"], NewSqlQuery(query, args...))
	return nil, nil
}
func (c *mockSqllibBackend) QueryRow(query string, args ...interface{}) *sql.Row {
	c.MethodsCalled["QueryRow"] = append(c.MethodsCalled["QueryRow"], NewSqlQuery(query, args...))
	return nil
}
