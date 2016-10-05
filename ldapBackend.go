package onedb

import (
	"crypto/tls"
	"errors"
	"fmt"

	"gopkg.in/ldap.v2"
)

var errInvalidLdapQueryType = errors.New("Invalid query. Must be of type *ldap.SearchRequest")
var errInvalidLdapExecType = errors.New("Invalid execute request. Must be of type *ldap.AddRequest, *ldap.DelRequest, *ldap.ModifyRequest, *ldap.SimpleBindRequest or *ldap.PasswordModifyRequest")
var ldapCreate ldapCreator = &ldapRealCreator{}

type ldapCreator interface {
	Dial(network, addr string) (ldapBackender, error)
}

type ldapRealCreator struct{}

func (d *ldapRealCreator) Dial(network, addr string) (ldapBackender, error) {
	conn, err := ldap.Dial(network, addr)
	conn.Debug = true
	return conn, err
}

type ldapBackend struct {
	l ldapBackender
}

type ldapBackender interface {
	StartTLS(config *tls.Config) error
	Bind(username, password string) error
	SimpleBind(simpleBindRequest *ldap.SimpleBindRequest) (*ldap.SimpleBindResult, error)
	Close()
	Add(addRequest *ldap.AddRequest) error
	Del(delRequest *ldap.DelRequest) error
	Modify(modifyRequest *ldap.ModifyRequest) error
	PasswordModify(passwordModifyRequest *ldap.PasswordModifyRequest) (*ldap.PasswordModifyResult, error)
	Search(searchRequest *ldap.SearchRequest) (*ldap.SearchResult, error)
	//	SearchWithPaging(searchRequest *ldap.SearchRequest, pagingSize uint32) (*ldap.SearchResult, error)
}

func NewLdap(hostname string, port int, binddn string, password string) (DBer, error) {
	l, err := ldapCreate.Dial("tcp", fmt.Sprintf("%s:%d", hostname, port))
	if err != nil {
		return nil, err
	}

	if err = l.StartTLS(&tls.Config{ServerName: hostname}); err != nil {
		return nil, err
	}

	if err := l.Bind(binddn, password); err != nil {
		return nil, err
	}

	return newBackendConverter(&ldapBackend{l: l}), nil
}

func (l *ldapBackend) Backend() interface{} {
	return l.l
}

func (l *ldapBackend) Close() error {
	l.l.Close()
	return nil
}

func (l *ldapBackend) Execute(query interface{}) error {
	switch r := query.(type) {
	case *ldap.AddRequest:
		return l.l.Add(r)
	case *ldap.DelRequest:
		return l.l.Del(r)
	case *ldap.ModifyRequest:
		return l.l.Modify(r)
	case *ldap.PasswordModifyRequest:
		return l.PasswordModify(r)
	case *ldap.SimpleBindRequest:
		return l.SimpleBind(r)
	default:
		return errInvalidLdapExecType
	}
}

func (l *ldapBackend) PasswordModify(r *ldap.PasswordModifyRequest) error {
	_, err := l.l.PasswordModify(r)
	return err
}

func (l *ldapBackend) SimpleBind(r *ldap.SimpleBindRequest) error {
	_, err := l.l.SimpleBind(r)
	return err
}

func (l *ldapBackend) Query(query interface{}) (rowsScanner, error) {
	q, ok := query.(*ldap.SearchRequest)
	if !ok {
		return nil, errInvalidLdapQueryType
	}
	res, err := l.l.Search(q)
	if err != nil {
		return nil, err
	}
	return newLdapRows(res.Entries), nil
}

type ldapRows struct {
	rows       []*ldap.Entry
	currentRow int
	rowsScanner
}

func newLdapRows(rows []*ldap.Entry) *ldapRows {
	return &ldapRows{rows: rows, currentRow: -1}
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
	if err := r.Err(); err != nil {
		return err
	} else if r.currentRow < 0 {
		return errors.New("Must call Next method before Scan")
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
