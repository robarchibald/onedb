package onedb

import (
	"crypto/tls"
	"errors"
	"gopkg.in/ldap.v2"
	"testing"
)

func TestNewLdap(t *testing.T) {
	ldapCreate = &ldapMockCreator{}
	pgxCreate = &MockConnPoolNewer{}
	_, err := NewPgx("localhost", 5432, "user", "password", "database")
	if err != nil {
		t.Error("expected success")
	}

	pgxCreate = &MockConnPoolNewer{Err: errors.New("fail")}
	_, err = NewPgx("localhost", 5432, "user", "password", "database")
	if err == nil {
		t.Error("expected fail")
	}
}

func TestNewLdapDBRealConnection(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	pgxCreate = &pgxRealCreator{}
	_, err := NewPgx("localhost", 5432, "user", "password", "database")
	if err != nil {
		t.Error("expected connection success", err)
	}
}

func TestLdapClose(t *testing.T) {
	c := NewMockPgxConnector()
	d := &pgxBackend{db: c}
	d.Close()
	if len(c.MethodsCalled) != 1 || len(c.MethodsCalled["Close"]) != 1 {
		t.Error("expected close method to be called on backend")
	}
}

func TestLdapQueryJson(t *testing.T) {
	c := NewMockPgxConnector()
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

/***************************** MOCKS ****************************/
type ldapMockCreator struct {
	conn ldapBackender
	Err  error
}

func (l *ldapMockCreator) Dial(network, addr string) (ldapBackender, error) {
	if l.conn == nil {
		l.conn = newMockLdap()
	}
	return l.conn, l.Err
}

func (l *ldapMockCreator) NewSearchRequest(BaseDN string, Scope, DerefAliases, SizeLimit, TimeLimit int, TypesOnly bool, Filter string, Attributes []string, Controls []ldap.Control) *ldap.SearchRequest {
	return nil
}

type mockLdapBackend struct {
	MethodsCalled map[string][]interface{}
}

func newMockLdap() *mockLdapBackend {
	return &mockLdapBackend{MethodsCalled: make(map[string][]interface{})}
}

func (l *mockLdapBackend) Close() {
	l.methodCalled("Close", nil)
}
func (l *mockLdapBackend) StartTLS(config *tls.Config) error {
	l.methodCalled("StartTLS", config)
	return nil
}

func (l *mockLdapBackend) Bind(username, password string) error {
	l.methodCalled("Bind", username, password)
	return nil
}
func (l *mockLdapBackend) Search(searchRequest *ldap.SearchRequest) (*ldap.SearchResult, error) {
	l.methodCalled("Search", searchRequest)
	return nil, nil
}

func (l *mockLdapBackend) Add(addRequest *ldap.AddRequest) error {
	l.methodCalled("Add", addRequest)
	return nil
}
func (l *mockLdapBackend) Del(delRequest *ldap.DelRequest) error {
	l.methodCalled("Del", delRequest)
	return nil
}
func (l *mockLdapBackend) Modify(modifyRequest *ldap.ModifyRequest) error {
	l.methodCalled("Modify", modifyRequest)
	return nil
}
func (l *mockLdapBackend) PasswordModify(passwordModifyRequest *ldap.PasswordModifyRequest) (*ldap.PasswordModifyResult, error) {
	l.methodCalled("PasswordModify", passwordModifyRequest)
	return nil, nil
}

func (l *mockLdapBackend) methodCalled(name string, args ...interface{}) {
	l.MethodsCalled[name] = append(l.MethodsCalled[name], args)
}
