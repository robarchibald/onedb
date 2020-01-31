package pgx

import (
	"io"
	"testing"

	"github.com/EndFirstCorp/onedb"
)

type mockBackend struct {
	db          onedb.Mocker
	CopyFromErr error
	ExecErr     error
	PGXer
}

// Mocker is the interface for mocking and includes all of the PGXer interface plus 3 methods to make testing easier
type Mocker interface {
	PGXer
	QueriesRun() []onedb.MethodsRun
	SaveMethodCall(name string, arguments []interface{})
	VerifyNextCommand(t *testing.T, name string, expected ...interface{})
}

// NewMock returns a Mock PGX instance from a set of parameters
func NewMock(copyFromErr, execErr error, data ...interface{}) Mocker {
	return &mockBackend{db: onedb.NewMock(copyFromErr, execErr, data...)}
}

func (b *mockBackend) Begin() {}
func (b *mockBackend) Close() {}
func (b *mockBackend) Exec(query string, args ...interface{}) (CommandTag, error) {
	b.db.SaveMethodCall("Exec", append([]interface{}{query}, args...))
	return "", b.ExecErr
}
func (b *mockBackend) Query(query string, args ...interface{}) (onedb.RowsScanner, error) {
	return b.db.Query(query, args...)
}
func (b *mockBackend) QueryRow(query string, args ...interface{}) onedb.Scanner {
	return b.db.QueryRow(query, args...)
}
func (b *mockBackend) CopyFrom(tableName Identifier, columnNames []string, rowSrc CopyFromSource) (int, error) {
	b.db.SaveMethodCall("Exec", []interface{}{tableName, columnNames, rowSrc})
	return 0, b.CopyFromErr
}
func (b *mockBackend) QueryValues(query *onedb.Query, result ...interface{}) error {
	return onedb.QueryValues(b, query, result...)
}
func (b *mockBackend) QueryJSON(query string, args ...interface{}) (string, error) {
	return onedb.QueryJSON(b, query, args...)
}
func (b *mockBackend) QueryJSONRow(query string, args ...interface{}) (string, error) {
	return onedb.QueryJSONRow(b, query, args...)
}
func (b *mockBackend) QueryStruct(result interface{}, query string, args ...interface{}) error {
	return onedb.QueryStruct(b, result, query, args...)
}
func (b *mockBackend) QueryStructRow(result interface{}, query string, args ...interface{}) error {
	return onedb.QueryStructRow(b, result, query, args...)
}
func (b *mockBackend) QueryWriteCSV(w io.Writer, options onedb.CSVOptions, query string, args ...interface{}) error {
	return onedb.QueryWriteCSV(w, options, b, query, args...)
}
func (b *mockBackend) QueriesRun() []onedb.MethodsRun {
	return b.db.QueriesRun()
}
func (b *mockBackend) SaveMethodCall(name string, arguments []interface{}) {
	b.db.SaveMethodCall(name, arguments)
}
func (b *mockBackend) VerifyNextCommand(t *testing.T, name string, expected ...interface{}) {
	b.db.VerifyNextCommand(t, name, expected...)
}
