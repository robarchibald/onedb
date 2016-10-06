package onedb

import (
	"crypto/tls"
	"errors"
	"gopkg.in/ldap.v2"
	"testing"
)

func TestNewLdap(t *testing.T) {
	ldapCreate = &ldapMockCreator{}
	_, err := NewLdap("localhost", 389, "user", "password")
	if err != nil {
		t.Error("expected success")
	}

	ldapCreate = &ldapMockCreator{Err: errors.New("fail")}
	_, err = NewLdap("localhost", 389, "user", "password")
	if err == nil {
		t.Error("expected fail")
	}

	l := newMockLdap()
	l.StartTLSErr = errors.New("Fail")
	ldapCreate = &ldapMockCreator{conn: l}
	_, err = NewLdap("localhost", 389, "user", "password")
	if err == nil {
		t.Error("expected fail on StartTLS")
	}

	l.StartTLSErr = nil
	l.BindErr = errors.New("fail")
	_, err = NewLdap("localhost", 389, "user", "password")
	if err == nil {
		t.Error("Expected Bind error")
	}
}

func TestNewLdapDBRealConnection(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	ldapCreate = &ldapRealCreator{}
	_, err := NewLdap("localhost", 389, "user", "password")
	if err != nil {
		t.Error("expected connection success", err)
	}
}

func TestLdapClose(t *testing.T) {
	m := newMockLdap()
	d := &ldapBackend{l: m}
	d.Close()
	if len(m.MethodsCalled) != 1 || len(m.MethodsCalled["Close"]) != 1 {
		t.Error("expected close method to be called on backend")
	}
}

func TestLdapBackend(t *testing.T) {
	m := newMockLdap()
	d := &ldapBackend{l: m}
	b := d.Backend()
	if b != m {
		t.Error("expected backend to match the created mock")
	}
}

func TestLdapQuery(t *testing.T) {
	m := newMockLdap()
	l := &ldapBackend{l: m}
	_, err := l.Query("bogus")
	if err == nil || err != errInvalidLdapQueryType {
		t.Error("expected error")
	}

	m.SearchErr = errors.New("fail")
	r := ldap.NewSearchRequest("baseDn", ldap.ScopeSingleLevel, ldap.NeverDerefAliases, 0, 0, false, "filter", []string{"attributes"}, nil)
	_, err = l.Query(r)
	queries := m.MethodsCalled["Search"]
	if err == nil || len(m.MethodsCalled) != 1 || len(queries) != 1 {
		t.Error("expected Search method to be called on backend and return err")
	}

	m.SearchErr = nil
	entries := []*ldap.Entry{&ldap.Entry{DN: "item1"}, &ldap.Entry{DN: "item2"}}
	m.SearchReturn = &ldap.SearchResult{Entries: entries}
	s, err := l.Query(r)
	if rows := s.Entries; len(rows) != len(entries) || rows[0].DN != "item1" || rows[1].DN != "item2" {
		t.Error("expected rows that were passed in")
	}
}

type ldapQueryStruct struct {
	Test  []string
	Test2 []string
}

func TestLdapQueryStruct(t *testing.T) {
	m := newMockLdap()
	a1 := ldap.EntryAttribute{Name: "Test", Values: []string{"1", "2"}}
	a2 := ldap.EntryAttribute{Name: "Test2", Values: []string{"3", "4"}}
	entries := []*ldap.Entry{&ldap.Entry{DN: "item1", Attributes: []*ldap.EntryAttribute{&a1, &a2}}, &ldap.Entry{DN: "item2", Attributes: []*ldap.EntryAttribute{&a2, &a1}}}
	m.SearchReturn = &ldap.SearchResult{Entries: entries}
	l := &ldapBackend{l: m}
	r := ldap.NewSearchRequest("baseDn", ldap.ScopeSingleLevel, ldap.NeverDerefAliases, 0, 0, false, "filter", []string{"attributes"}, nil)
	d := []ldapQueryStruct{}
	err := l.QueryStruct(r, &d)
	if err != nil || d[0].Test[0] != "1" || d[0].Test[1] != "2" || d[1].Test[0] != "1" || d[1].Test[1] != "2" || d[0].Test2[0] != "3" || d[0].Test2[1] != "4" || d[1].Test2[0] != "3" || d[1].Test2[1] != "4" {
		t.Error("expected error", err, d)
	}
}

func TestLdapExecute(t *testing.T) {
	m := newMockLdap()
	l := &ldapBackend{l: m}
	err := l.Execute("bogus")
	if err == nil || err != errInvalidLdapExecType {
		t.Error("expected error")
	}

	m.AddErr = errors.New("fail")
	err = l.Execute(ldap.NewAddRequest("Dn"))
	if err == nil || len(m.MethodsCalled["Add"]) != 1 {
		t.Error("expected Add method to be called on backend and return err")
	}

	m.DelErr = errors.New("fail")
	err = l.Execute(ldap.NewDelRequest("Dn", nil))
	if err == nil || len(m.MethodsCalled["Del"]) != 1 {
		t.Error("expected Del method to be called on backend and return err")
	}

	m.ModifyErr = errors.New("fail")
	err = l.Execute(ldap.NewModifyRequest("Dn"))
	if err == nil || len(m.MethodsCalled["Modify"]) != 1 {
		t.Error("expected Modify method to be called on backend and return err")
	}

	m.PasswordModifyErr = errors.New("fail")
	err = l.Execute(ldap.NewPasswordModifyRequest("Dn", "oldPassword", "newPassword"))
	if err == nil || len(m.MethodsCalled["PasswordModify"]) != 1 {
		t.Error("expected PasswordModify method to be called on backend and return err")
	}
}

/*func TestLdapRowsScanAndNext(t *testing.T) {
	var uid, password, uidNumber, gidNumber, home interface{}
	entries := []*ldap.Entry{
		&ldap.Entry{DN: "item1", Attributes: []*ldap.EntryAttribute{
			&ldap.EntryAttribute{Name: "uid", Values: []string{"rob@robarchibald.com"}},
			&ldap.EntryAttribute{Name: "userPassword", Values: []string{"password"}},
			&ldap.EntryAttribute{Name: "uidNumber", Values: []string{"1001"}},
			&ldap.EntryAttribute{Name: "gidNumber", Values: []string{"10001"}},
			&ldap.EntryAttribute{Name: "homeDirectory", Values: []string{"/homeDir/robarchibald.com/rob"}},
		}},
		&ldap.Entry{DN: "item2", Attributes: []*ldap.EntryAttribute{
			&ldap.EntryAttribute{Name: "uid", Values: []string{"rob.archibald@endfirst.com"}},
			&ldap.EntryAttribute{Name: "userPassword", Values: []string{"password"}},
			&ldap.EntryAttribute{Name: "uidNumber", Values: []string{"1002"}},
			&ldap.EntryAttribute{Name: "gidNumber", Values: []string{"10002"}},
			&ldap.EntryAttribute{Name: "homeDirectory", Values: []string{"/homeDir/endfirst.com/rob.archibald"}},
		}}}
	r := newLdapRows(entries)
	err := r.Scan(&uid, &password, &uidNumber, &gidNumber, &home)
	if err == nil {
		t.Error("expected error since we're not at first row yet")
	}

	r.Next()
	r.Scan(&uid, &password, &uidNumber, &gidNumber, &home)
	if len(uid.([]string)) != 1 || len(password.([]string)) != 1 || len(uidNumber.([]string)) != 1 || len(gidNumber.([]string)) != 1 || len(home.([]string)) != 1 ||
		uid.([]string)[0] != "rob@robarchibald.com" || password.([]string)[0] != "password" || uidNumber.([]string)[0] != "1001" || gidNumber.([]string)[0] != "10001" || home.([]string)[0] != "/homeDir/robarchibald.com/rob" {
		t.Error("expected valid values")
	}
	r.Next()
	r.Scan(&uid, &password, &uidNumber, &gidNumber, &home)
	if len(uid.([]string)) != 1 || len(password.([]string)) != 1 || len(uidNumber.([]string)) != 1 || len(gidNumber.([]string)) != 1 || len(home.([]string)) != 1 ||
		uid.([]string)[0] != "rob.archibald@endfirst.com" || password.([]string)[0] != "password" || uidNumber.([]string)[0] != "1002" || gidNumber.([]string)[0] != "10002" || home.([]string)[0] != "/homeDir/endfirst.com/rob.archibald" {
		t.Error("expected valid values")
	}
	r.Next()
	err = r.Scan(&uid, &password, &uidNumber, &gidNumber, &home)
	if err == nil {
		t.Error("expected error since we are past the final row")
	}
}

func TestLdapRowsClose(t *testing.T) {
	r := &ldapRows{}
	if r.Close() != nil {
		t.Error("expected success")
	}
}

func TestLdapRowsColumns(t *testing.T) {
	r := &ldapRows{}
	cols, _ := r.Columns()
	if len(cols) != 0 {
		t.Error("expected 0 columns")
	}

	entries := []*ldap.Entry{&ldap.Entry{DN: "item1", Attributes: []*ldap.EntryAttribute{
		&ldap.EntryAttribute{Name: "uid", Values: []string{"rob@robarchibald.com"}},
		&ldap.EntryAttribute{Name: "userPassword", Values: []string{"password"}},
		&ldap.EntryAttribute{Name: "uidNumber", Values: []string{"1001"}},
		&ldap.EntryAttribute{Name: "gidNumber", Values: []string{"10001"}},
		&ldap.EntryAttribute{Name: "homeDirectory", Values: []string{"/homeDir/robarchibald.com/rob"}},
	}}}
	r = newLdapRows(entries)
	cols, _ = r.Columns()
	if len(cols) != 5 || cols[0] != "uid" || cols[1] != "userPassword" || cols[2] != "uidNumber" || cols[3] != "gidNumber" || cols[4] != "homeDirectory" {
		t.Error("expected 5 columns")
	}
}*/

/***************************** MOCKS ****************************/
type mockLdapData struct {
	Uid           string
	UserPassword  string
	UidNumber     string
	GidNumber     string
	HomeDirectory string
}

type ldapMockCreator struct {
	conn ldapBackender
	Err  error
}

func (l *ldapMockCreator) Dial(network, addr string) (ldapBackender, error) {
	if l.conn == nil && l.Err == nil {
		l.conn = newMockLdap()
	}
	return l.conn, l.Err
}

type mockLdapBackend struct {
	MethodsCalled     map[string][]interface{}
	SearchReturn      *ldap.SearchResult
	StartTLSErr       error
	BindErr           error
	SearchErr         error
	AddErr            error
	DelErr            error
	ModifyErr         error
	PasswordModifyErr error
}

func newMockLdap() *mockLdapBackend {
	return &mockLdapBackend{MethodsCalled: make(map[string][]interface{})}
}

func (l *mockLdapBackend) Close() {
	l.methodCalled("Close", nil)
}
func (l *mockLdapBackend) StartTLS(config *tls.Config) error {
	l.methodCalled("StartTLS", config)
	return l.StartTLSErr
}

func (l *mockLdapBackend) Bind(username, password string) error {
	l.methodCalled("Bind", username, password)
	return l.BindErr
}

func (l *mockLdapBackend) SimpleBind(r *ldap.SimpleBindRequest) (*ldap.SimpleBindResult, error) {
	l.methodCalled("SimpleBind", r)
	return nil, l.BindErr
}

func (l *mockLdapBackend) Search(searchRequest *ldap.SearchRequest) (*ldap.SearchResult, error) {
	l.methodCalled("Search", searchRequest)
	return l.SearchReturn, l.SearchErr
}

func (l *mockLdapBackend) Add(addRequest *ldap.AddRequest) error {
	l.methodCalled("Add", addRequest)
	return l.AddErr
}
func (l *mockLdapBackend) Del(delRequest *ldap.DelRequest) error {
	l.methodCalled("Del", delRequest)
	return l.DelErr
}
func (l *mockLdapBackend) Modify(modifyRequest *ldap.ModifyRequest) error {
	l.methodCalled("Modify", modifyRequest)
	return l.ModifyErr
}
func (l *mockLdapBackend) PasswordModify(passwordModifyRequest *ldap.PasswordModifyRequest) (*ldap.PasswordModifyResult, error) {
	l.methodCalled("PasswordModify", passwordModifyRequest)
	return nil, l.PasswordModifyErr
}

func (l *mockLdapBackend) methodCalled(name string, args ...interface{}) {
	l.MethodsCalled[name] = append(l.MethodsCalled[name], args)
}
