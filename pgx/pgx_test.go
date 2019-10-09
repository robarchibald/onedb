package pgx

import (
	"errors"
	"reflect"
	"testing"

	"github.com/EndFirstCorp/onedb"
	pgx "gopkg.in/jackc/pgx.v2"
)

func TestNewPgxFromURI(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	_, err := NewPgxFromURI("http%20://bogus")
	if err == nil {
		t.Error("expected connection error")
	}

	_, err = NewPgxFromURI("")
	if err != nil {
		t.Error("expected success", err)
	}
}

func TestNewPgx(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	_, err := NewPgx("localhost", 5432, "user", "password", "database")
	if err != nil {
		t.Error("expected connection success", err)
	}
}

func TestPgxClose(t *testing.T) {
	c := newMockPgx(nil, nil)
	d := &pgxBackend{db: c}
	d.Close()
	if len(c.MethodsCalled) != 1 || len(c.MethodsCalled["Close"]) != 1 {
		t.Error("expected close method to be called on backend")
	}
}

func TestPgxQuery(t *testing.T) {
	c := newMockPgx(nil, nil)
	d := &pgxBackend{db: c}

	d.Query("query", "arg1", "arg2")
	queries := c.MethodsCalled["Query"]
	if len(c.MethodsCalled) != 1 || len(queries) != 1 {
		t.Fatal("expected query method to be called on backend")
	}
	verifyArgs(t, queries[0], "query", "arg1", "arg2")
}

func verifyArgs(t *testing.T, actual []interface{}, expected ...interface{}) {
	if len(expected) != len(actual) {
		t.Fatal("Number of arguments don't match. Expected:", len(expected), expected, "actual:", len(actual), actual)
	}
	for i := range actual {
		if !reflect.DeepEqual(actual[i], expected[i]) {
			t.Errorf("Argument mismatch at %d. Expected:%v, Actual:%v\n", i, expected[i], actual[i])
		}
	}
}

func TestPgxQueryRow(t *testing.T) {
	c := newMockPgx(nil, nil)
	d := &pgxBackend{db: c}

	d.QueryRow("query", "arg1", "arg2")
	queries := c.MethodsCalled["QueryRow"]
	if len(c.MethodsCalled) != 1 || len(queries) != 1 {
		t.Fatal("expected query method to be called on backend")
	}
	verifyArgs(t, queries[0], "query", "arg1", "arg2")
}

func TestPgxCopyFrom(t *testing.T) {
	c := newMockPgx(nil, nil)
	d := &pgxBackend{db: c}

	d.CopyFrom(nil, nil, nil)
	queries := c.MethodsCalled["CopyFrom"]
	if len(c.MethodsCalled) != 1 || len(queries) != 1 {
		t.Fatal("expected query method to be called on backend")
	}
}

func TestPgxExec(t *testing.T) {
	c := newMockPgx(nil, nil)
	d := &pgxBackend{db: c}

	d.Exec("query", "arg1", "arg2")
	queries := c.MethodsCalled["Exec"]
	if len(c.MethodsCalled) != 1 || len(queries) != 1 {
		t.Fatal("expected Exec method to be called on backend")
	}
	verifyArgs(t, queries[0], "query", "arg1", "arg2")
}

func TestPgxQueryValues(t *testing.T) {
	c := newMockPgx(nil, &SimpleData{IntVal: 1, StringVal: "hello"})
	d := &pgxBackend{db: c}

	var intVal int
	var stringVal string
	q := onedb.NewQuery("query", "arg1", "arg2")
	d.QueryValues(q, &intVal, &stringVal)
	queries := c.MethodsCalled["QueryRow"]
	if len(c.MethodsCalled) != 1 || len(queries) != 1 {
		t.Fatal("expected QueryRow method to be called on backend")
	}
	if intVal != 1 || stringVal != "hello" {
		t.Error("expected values to be set", intVal, stringVal)
	}
	verifyArgs(t, queries[0], "query", "arg1", "arg2")
}

func TestPgxQueryJSON(t *testing.T) {
	c := newMockPgx([]SimpleData{{IntVal: 1, StringVal: "hello"}}, nil)
	d := &pgxBackend{db: c}

	json, err := d.QueryJSON("query", "arg1", "arg2")
	if err != nil || json != `[{"IntVal":1,"StringVal":"hello"}]` {
		t.Error("expected valid results", json, err)
	}
	queries := c.MethodsCalled["Query"]
	if len(c.MethodsCalled) != 1 || len(queries) != 1 {
		t.Fatal("expected Query method to be called on backend")
	}
	verifyArgs(t, queries[0], "query", "arg1", "arg2")
}

func TestPgxQueryJSONRow(t *testing.T) {
	c := newMockPgx([]SimpleData{{IntVal: 1, StringVal: "hello"}}, nil)
	d := &pgxBackend{db: c}

	json, err := d.QueryJSONRow("query", "arg1", "arg2")
	if err != nil || json != `{"IntVal":1,"StringVal":"hello"}` {
		t.Error("expected valid results", json, err)
	}
	queries := c.MethodsCalled["Query"]
	if len(c.MethodsCalled) != 1 || len(queries) != 1 {
		t.Fatal("expected Query method to be called on backend")
	}
	verifyArgs(t, queries[0], "query", "arg1", "arg2")
}

func TestPgxQueryStruct(t *testing.T) {
	c := newMockPgx([]SimpleData{{IntVal: 1, StringVal: "hello"}}, nil)
	d := &pgxBackend{db: c}

	r := []SimpleData{}
	err := d.QueryStruct(&r, "query", "arg1", "arg2")
	if err != nil || len(r) != 1 || r[0].IntVal != 1 || r[0].StringVal != "hello" {
		t.Error("expected valid results", err)
	}
	queries := c.MethodsCalled["Query"]
	if len(c.MethodsCalled) != 1 || len(queries) != 1 {
		t.Fatal("expected Query method to be called on backend")
	}
	verifyArgs(t, queries[0], "query", "arg1", "arg2")
}

func TestPgxQueryStructRow(t *testing.T) {
	c := newMockPgx([]SimpleData{{IntVal: 1, StringVal: "hello"}}, nil)
	d := &pgxBackend{db: c}

	r := SimpleData{}
	err := d.QueryStructRow(&r, "query", "arg1", "arg2")
	if err != nil || r.IntVal != 1 || r.StringVal != "hello" {
		t.Error("expected valid results", err)
	}
	queries := c.MethodsCalled["Query"]
	if len(c.MethodsCalled) != 1 || len(queries) != 1 {
		t.Fatal("expected Query method to be called on backend")
	}
	verifyArgs(t, queries[0], "query", "arg1", "arg2")
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

func TestPgxRowsValues(t *testing.T) {
	m := newMockPgxRows()
	m.ValuesData = []interface{}{"hello", "world"}
	r := &pgxRows{rows: m}
	v, err := r.Values()
	if len(m.MethodsCalled["Values"]) != 1 || len(v) != 2 || err != nil {
		t.Error("expected Values method to be called", v, err)
	}

	m.ValuesErr = errors.New("fail")
	_, err = r.Values()
	if err == nil {
		t.Error("expected error")
	}

}

func TestPgxRowsScan(t *testing.T) {
	m := newMockPgxRows()
	r := &pgxRows{rows: m}
	r.Scan()
	if len(m.MethodsCalled["Values"]) != 1 {
		t.Error("expected Values method to be called")
	}

	m.ValuesErr = errors.New("fail")
	err := r.Scan()
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
type mockPgx struct {
	MethodsCalled  map[string][][]interface{}
	QueryReturn    onedb.RowsScanner
	QueryRowReturn onedb.Scanner
}

func newMockPgx(rows interface{}, row interface{}) *mockPgx {
	pgx := &mockPgx{MethodsCalled: make(map[string][][]interface{})}
	if rows != nil {
		pgx.QueryReturn = onedb.NewRowsScanner(rows)
	}
	if row != nil {
		pgx.QueryRowReturn = onedb.NewScanner(row)
	}
	return pgx
}

func (c *mockPgx) Close() {
	c.MethodsCalled["Close"] = append(c.MethodsCalled["Close"], nil)
}
func (c *mockPgx) Exec(query string, args ...interface{}) (CommandTag, error) {
	c.MethodsCalled["Exec"] = append(c.MethodsCalled["Exec"], append([]interface{}{query}, args...))
	return "tag", nil
}
func (c *mockPgx) Query(query string, args ...interface{}) (onedb.RowsScanner, error) {
	c.MethodsCalled["Query"] = append(c.MethodsCalled["Query"], append([]interface{}{query}, args...))
	return c.QueryReturn, nil
}

func (c *mockPgx) QueryRow(query string, args ...interface{}) onedb.Scanner {
	c.MethodsCalled["QueryRow"] = append(c.MethodsCalled["QueryRow"], append([]interface{}{query}, args...))
	return c.QueryRowReturn
}

func (c *mockPgx) CopyFrom(tableName Identifier, columnNames []string, rows CopyFromSource) (int, error) {
	c.MethodsCalled["CopyFrom"] = append(c.MethodsCalled["CopyFrom"], []interface{}{tableName, columnNames, rows})
	return 0, nil
}

type mockPgxRows struct {
	MethodsCalled map[string][]interface{}
	ValuesData    []interface{}
	ValuesErr     error
	ScanErr       error
}

func newMockPgxRows() *mockPgxRows {
	return &mockPgxRows{MethodsCalled: make(map[string][]interface{})}
}

func (r *mockPgxRows) AfterClose(f func(*pgx.Rows)) {
	r.MethodsCalled["AfterClose"] = append(r.MethodsCalled["AfterClose"], f)
}
func (r *mockPgxRows) Close() {
	r.MethodsCalled["Close"] = append(r.MethodsCalled["Close"], nil)
}
func (r *mockPgxRows) Conn() *pgx.Conn {
	r.MethodsCalled["Conn"] = append(r.MethodsCalled["Conn"], nil)
	return nil
}
func (r *mockPgxRows) Err() error {
	r.MethodsCalled["Err"] = append(r.MethodsCalled["Err"], nil)
	return nil
}
func (r *mockPgxRows) Fatal(err error) {
	r.MethodsCalled["Fatal"] = append(r.MethodsCalled["Fatal"], err)
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
	return r.ValuesData, r.ValuesErr
}
func (r *mockPgxRows) Scan(result ...interface{}) error {
	r.MethodsCalled["Scan"] = append(r.MethodsCalled["Scan"], nil)
	return r.ScanErr
}

type SimpleData struct {
	IntVal    int
	StringVal string
}
