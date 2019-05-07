package redis

import (
	"errors"
	"reflect"
	"testing"

	"github.com/EndFirstCorp/onedb"
)

type redisMock struct {
	db       onedb.MockDBer
	DoResult interface{}
	DoErr    error
	DelErr   error
	SetErr   error
	Rediser
}

// Mocker interface includes all the Rediser interface, plus three additional methods to help with testing
type Mocker interface {
	Rediser
	QueriesRun() []onedb.MethodsRun
	SaveMethodCall(name string, arguments []interface{})
	VerifyNextCommand(t *testing.T, name string, expected ...interface{})
}

// NewMock is the constructor for a fake Redis connection
func NewMock(delErr, saveErr error, doResult interface{}, doErr error) Mocker {
	return &redisMock{db: onedb.NewMock(nil, nil, doResult)}
}

func (r *redisMock) Close() error {
	r.db.SaveMethodCall("Close", nil)
	return nil
}

func (r *redisMock) Get(key string) (string, error) {
	return r.db.QueryJSON(key)
}

func (r *redisMock) GetStruct(key string, result interface{}) error {
	resultType := reflect.TypeOf(result)
	if !onedb.IsPointer(resultType) {
		return errors.New("invalid result type")
	}
	if onedb.IsSlice(resultType.Elem()) {
		return r.db.QueryStruct(result, key)
	} else if onedb.IsStruct(resultType.Elem()) {
		return r.db.QueryStructRow(result, key)
	}
	return errors.New("invalid result type")
}

func (r *redisMock) SetWithExpire(key string, value interface{}, expireSeconds int) error {
	r.db.SaveMethodCall("SetWithExpire", []interface{}{value, expireSeconds})
	return r.SetErr
}

func (r *redisMock) Del(key string) error {
	r.db.SaveMethodCall("Del", []interface{}{key})
	return r.DelErr
}

func (r *redisMock) Do(command string, args ...interface{}) (interface{}, error) {
	r.db.SaveMethodCall("Do", append([]interface{}{command}, args...))
	return r.DoResult, r.DoErr
}

func (r *redisMock) QueriesRun() []onedb.MethodsRun {
	return r.db.QueriesRun()
}
func (r *redisMock) SaveMethodCall(name string, arguments []interface{}) {
	r.db.SaveMethodCall(name, arguments)
}
func (r *redisMock) VerifyNextCommand(t *testing.T, name string, expected ...interface{}) {
	r.db.VerifyNextCommand(t, name, expected...)
}
