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
	sqlOpen = &MockSqlOpener{}
	_, err := NewSqllib("mssql", connectionString)
	if err != nil {
		t.Error("expected success")
	}

	sqlOpen = &MockSqlOpener{Err: errors.New("fail")}
	_, err = NewSqllib("mssql", connectionString)
	if err == nil {
		t.Error("expected error")
	}
}

func TestNewSqllibBackend(t *testing.T) {
	sqlOpen = &MockSqlOpener{connector: &MockSqlBackend{PingErr: errors.New("fail")}}
	_, err := newSqllibBackend("mssql", connectionString)
	if err == nil {
		t.Error("expected fail on ping")
	}
}

func TestNewSqllibOneDBRealConnection(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	sqlOpen = &SqllibOpener{}
	_, err := NewSqllib("mssql", connectionString)
	if err != nil {
		t.Error("expected connection success", err)
	}
}

func TestSqllibClose(t *testing.T) {
	c := NewMockSqlConnector()
	d := &SqllibBackend{db: c}
	d.Close()
	if len(c.MethodsCalled) != 1 || len(c.MethodsCalled["Close"]) != 1 {
		t.Error("expected close method to be called on backend")
	}
}

func TestSqllibQuery(t *testing.T) {
	c := NewMockSqlConnector()
	d := &SqllibBackend{db: c}
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

func TestSqllibQueryRow(t *testing.T) {
	c := NewMockSqlConnector()
	d := &SqllibBackend{db: c}
	row := d.QueryRow("bogus")
	if row.Scan(nil) == nil {
		t.Error("expected error")
	}

	d.QueryRow(NewSqlQuery("query", "arg1", "arg2"))
	queries := c.MethodsCalled["QueryRow"]
	if len(c.MethodsCalled) != 1 || len(queries) != 1 ||
		queries[0].(*SqlQuery).query != "query" ||
		queries[0].(*SqlQuery).args[0] != "arg1" ||
		queries[0].(*SqlQuery).args[1] != "arg2" {
		t.Error("expected query method to be called on backend")
	}
}

/***************************** MOCKS ****************************/
type MockSqlOpener struct {
	connector SqlLibBackender
	Err       error
}

func (o *MockSqlOpener) Open(driverName, dataSourceName string) (SqlLibBackender, error) {
	if o.connector == nil {
		o.connector = NewMockSqlConnector()
	}
	return o.connector, o.Err
}

type MockSqlBackend struct {
	MethodsCalled map[string][]interface{}
	PingErr       error
}

func NewMockSqlConnector() *MockSqlBackend {
	return &MockSqlBackend{MethodsCalled: make(map[string][]interface{})}
}

func (c *MockSqlBackend) Ping() error {
	return c.PingErr
}

func (c *MockSqlBackend) Close() error {
	c.MethodsCalled["Close"] = append(c.MethodsCalled["Close"], nil)
	return nil
}
func (c *MockSqlBackend) Exec(query string, args ...interface{}) (sql.Result, error) {
	c.MethodsCalled["Exec"] = append(c.MethodsCalled["Exec"], NewSqlQuery(query, args...))
	return nil, nil
}
func (c *MockSqlBackend) Query(query string, args ...interface{}) (*sql.Rows, error) {
	c.MethodsCalled["Query"] = append(c.MethodsCalled["Query"], NewSqlQuery(query, args...))
	return nil, nil
}
func (c *MockSqlBackend) QueryRow(query string, args ...interface{}) *sql.Row {
	c.MethodsCalled["QueryRow"] = append(c.MethodsCalled["QueryRow"], NewSqlQuery(query, args...))
	return nil
}
