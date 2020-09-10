package sql

import (
	sqllib "database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/EndFirstCorp/onedb"
)

const connectionString string = "server=localhost;user id=gotest;password=go;database=GoTest;encrypt=disable"

func TestNewSqllibOneDB(t *testing.T) {
	openDatabase = newSqllibMockCreator(&mockSqllibBackend{}, nil)
	_, err := NewSqllib("mssql", connectionString)
	if err != nil {
		t.Error("expected success")
	}

	openDatabase = newSqllibMockCreator(nil, errors.New("fail"))
	_, err = NewSqllib("mssql", connectionString)
	if err == nil {
		t.Error("expected error")
	}

	openDatabase = newSqllibMockCreator(&mockSqllibBackend{PingErr: errors.New("fail")}, nil)
	_, err = NewSqllib("mssql", connectionString)
	if err == nil {
		t.Error("expected fail on ping")
	}
}

func TestNewSqllibOneDBRealConnection(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	openDatabase = sqllibOpen
	_, err := NewSqllib("mssql", connectionString)
	if err != nil {
		t.Error("expected connection success", err)
	}
}

func TestSqllibClose(t *testing.T) {
	c := newMockSqllibBackend()
	d := &sqllibBackend{db: c}
	d.Close()
	if len(c.MethodsRun) != 1 || c.MethodsRun[0].MethodName != "Close" {
		t.Error("expected close method to be called on backend")
	}
}

func TestSqllibQuery(t *testing.T) {
	c := newMockSqllibBackend()
	d := &sqllibBackend{db: c}

	d.Query("query", "arg1", "arg2")
	if len(c.MethodsRun) != 1 || c.MethodsRun[0].MethodName != "Query" {
		t.Fatal("expected query method to be called on backend")
	}
	verifyArgs(t, c.MethodsRun[0].Arguments, "query", "arg1", "arg2")
}

func TestSqllibExecute(t *testing.T) {
	c := newMockSqllibBackend()
	d := &sqllibBackend{db: c}

	d.Exec("query", "arg1", "arg2")
	if len(c.MethodsRun) != 1 || c.MethodsRun[0].MethodName != "Exec" {
		t.Fatal("expected Exec method to be called on backend")
	}
	verifyArgs(t, c.MethodsRun[0].Arguments, "query", "arg1", "arg2")
}

/***************************** MOCKS ****************************/
func newSqllibMockCreator(conn sqlLibBackender, err error) openDatabaseFunc {
	return func(driverName, dataSourceName string) (sqlLibBackender, error) {
		return conn, err
	}
}

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
	MethodsRun []onedb.MethodsRun
	PingErr    error
}

func newMockSqllibBackend() *mockSqllibBackend {
	return &mockSqllibBackend{}
}

func (c *mockSqllibBackend) SaveMethodCall(name string, arguments []interface{}) {
	c.MethodsRun = append(c.MethodsRun, onedb.MethodsRun{MethodName: name, Arguments: arguments})
}

func (c *mockSqllibBackend) Ping() error {
	return c.PingErr
}

func (c *mockSqllibBackend) Close() error {
	c.SaveMethodCall("Close", nil)
	return nil
}
func (c *mockSqllibBackend) Exec(query string, args ...interface{}) (sqllib.Result, error) {
	c.SaveMethodCall("Exec", append([]interface{}{query}, args...))
	return nil, nil
}
func (c *mockSqllibBackend) Query(query string, args ...interface{}) (*sqllib.Rows, error) {
	c.SaveMethodCall("Query", append([]interface{}{query}, args...))
	return nil, nil
}
func (c *mockSqllibBackend) QueryRow(query string, args ...interface{}) *sqllib.Row {
	c.SaveMethodCall("QueryRow", append([]interface{}{query}, args...))
	return nil
}

func verifyArgs(t *testing.T, actual []interface{}, expected ...interface{}) {
	if len(expected) != len(actual) {
		t.Fatal("Number of arguments don't match. Expected:", len(expected), "actual:", len(actual))
	}
	for i := range actual {
		if !reflect.DeepEqual(actual[i], expected[i]) {
			t.Errorf("Argument mismatch at %d. Expected:%v, Actual:%v\n", i, expected[i], actual[i])
		}
	}
}
