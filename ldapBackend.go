package onedb

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"

	"gopkg.in/ldap.v2"
)

var ldapCreate ldapCreator = &ldapRealCreator{}

type ldapCreator interface {
	Dial(network, addr string) (ldapBackender, error)
	NewSearchRequest(BaseDN string, Scope, DerefAliases, SizeLimit, TimeLimit int, TypesOnly bool, Filter string, Attributes []string, Controls []ldap.Control) *ldap.SearchRequest
}

type ldapRealCreator struct{}

func (d *ldapRealCreator) Dial(network, addr string) (ldapBackender, error) {
	return ldap.Dial(network, addr)
}

func (d *ldapRealCreator) NewSearchRequest(baseDN string, scope, derefAliases, sizeLimit, timeLimit int, typesOnly bool, filter string, attributes []string, controls []ldap.Control) *ldap.SearchRequest {
	return ldap.NewSearchRequest(baseDN, scope, derefAliases, sizeLimit, timeLimit, typesOnly, filter, attributes, controls)
}

type ldapBackend struct {
	l *ldap.Conn
}

type ldapBackender interface {
	StartTLS(config *tls.Config) error
	Bind(username, password string) error
	Close()
	Search(searchRequest *ldap.SearchRequest) (*ldap.SearchResult, error)
	Add(addRequest *ldap.AddRequest) error
	Del(delRequest *ldap.DelRequest) error
	Modify(modifyRequest *ldap.ModifyRequest) error
	PasswordModify(passwordModifyRequest *ldap.PasswordModifyRequest) (*ldap.PasswordModifyResult, error)
}

func NewLdap(hostname string, port int, binddn string, password string) (DBer, error) {
	l, err := ldap.Dial("tcp", fmt.Sprintf("%s:%d", hostname, port))
	if err != nil {
		return nil, err
	}

	if err = l.StartTLS(&tls.Config{ServerName: hostname}); err != nil {
		return nil, err
	}

	if err := l.Bind(binddn, password); err != nil {
		return nil, err
	}

	return &ldapBackend{l: l}, nil
}

func (l *ldapBackend) Backend() interface{} {
	return l.l
}

func (l *ldapBackend) Close() error {
	l.l.Close()
	return nil
}

func (l *ldapBackend) Execute(query interface{}) error {
	return nil
}

func (l *ldapBackend) QueryJSON(query interface{}) (string, error) {
	q, ok := query.(*ldap.SearchRequest)
	if !ok {
		return "", errInvalidQueryType
	}
	//[]string{"uid", "userPassword", "uidNumber", "gidNumber", "homeDirectory"}
	//baseDN, scope,     derefAliases,   sizeLimit,   timeLimit,   typesOnly,   filter,        attributes,   controls
	req := ldapCreate.NewSearchRequest(q.BaseDN, q.Scope, q.DerefAliases, q.SizeLimit, q.TimeLimit, q.TypesOnly, q.Filter, q.Attributes, q.Controls)
	res, err := l.l.Search(req)
	if err != nil {
		return "", err
	}
	data, err := json.Marshal(res.Entries)
	if err != nil {
		return "", err
	}

	return string(data), nil
}
func (l *ldapBackend) QueryJSONRow(query interface{}) (string, error) {
	q, ok := query.(*ldap.SearchRequest)
	if !ok {
		return "", errInvalidQueryType
	}
	//[]string{"uid", "userPassword", "uidNumber", "gidNumber", "homeDirectory"}
	//baseDN, scope,     derefAliases,   sizeLimit,   timeLimit,   typesOnly,   filter,        attributes,   controls
	req := ldapCreate.NewSearchRequest(q.BaseDN, q.Scope, q.DerefAliases, q.SizeLimit, q.TimeLimit, q.TypesOnly, q.Filter, q.Attributes, q.Controls)
	res, err := l.l.Search(req)
	if err != nil {
		return "", err
	}
	data, err := json.Marshal(res.Entries[0])
	if err != nil {
		return "", err
	}

	return string(data), nil
}
func (l *ldapBackend) QueryStruct(query interface{}, result interface{}) error {
	return nil
}
func (l *ldapBackend) QueryStructRow(query interface{}, result interface{}) error {
	return nil
}

type ldapRows struct {
	rows       []*ldap.Entry
	currentRow int
	rowsScanner
}

func (r *ldapRows) Columns() ([]string, error) {
	if len(r.rows) == 0 {
		return []string{}, nil
	}

	fields := r.rows[0].Attributes
	columns := make([]string, len(fields))
	for i, field := range fields {
		columns[i] = field.Name
	}
	return columns, nil
}

func (r *ldapRows) Next() bool {
	r.currentRow++
	if r.currentRow >= len(r.rows) {
		return false
	}
	return true
}

func (r *ldapRows) Close() error {
	return nil
}

func (r *ldapRows) Scan(dest ...interface{}) error {
	if r.currentRow >= len(r.rows) {
		return errors.New("Current Row not found")
	}
	vals := r.rows[r.currentRow].Attributes
	for i, item := range dest {
		*(item.(*interface{})) = vals[i].Values
	}
	return nil
}

func (r *ldapRows) Err() error {
	if r.currentRow >= len(r.rows) {
		return errors.New("Current Row not found")
	}
	return nil
}
