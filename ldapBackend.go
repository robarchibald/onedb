package onedb

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"gopkg.in/ldap.v2"
	"reflect"
	"strings"
)

var errInvalidLdapQueryType = errors.New("Invalid query. Must be of type *ldap.SearchRequest")
var errInvalidLdapExecType = errors.New("Invalid execute request. Must be of type *ldap.AddRequest, *ldap.DelRequest, *ldap.ModifyRequest or *ldap.PasswordModifyRequest")
var ldapCreate ldapCreator = &ldapRealCreator{}

type ldapCreator interface {
	Dial(network, addr string) (ldapBackender, error)
}

type ldapRealCreator struct{}

func (d *ldapRealCreator) Dial(network, addr string) (ldapBackender, error) {
	return ldap.Dial(network, addr)
}

type ldapBackend struct {
	l ldapBackender
}

type ldapBackender interface {
	StartTLS(config *tls.Config) error
	Bind(username, password string) error
	//SimpleBind(simpleBindRequest *ldap.SimpleBindRequest) (*ldap.SimpleBindResult, error)
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

	return &ldapBackend{l: l}, nil
}

func (c *ldapBackend) QueryJSON(query interface{}) (string, error) {
	res, err := c.Query(query)
	if err != nil {
		return "", err
	}

	data, err := json.Marshal(res.Entries)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (c *ldapBackend) QueryJSONRow(query interface{}) (string, error) {
	res, err := c.Query(query)
	if err != nil {
		return "", err
	}

	data, err := json.Marshal(res.Entries[0])
	if err != nil {
		return "", err
	}
	return string(data), nil
}

type fieldInfo struct {
	Name  string
	Index int
}

func (c *ldapBackend) QueryStruct(query interface{}, result interface{}) error {
	resultType := reflect.TypeOf(result)
	if result == nil || !isPointer(resultType) || !isSlice(resultType.Elem()) {
		return errors.New("Invalid result argument.  Must be a pointer to a slice")
	}

	res, err := c.Query(query)
	if err != nil {
		return err
	}
	sliceValue := reflect.ValueOf(result).Elem() // from pointer to slice
	itemType := sliceValue.Type().Elem()
	fields := getFieldMap(itemType)
	for i := range res.Entries {
		resultValue := reflect.New(itemType)
		row := res.Entries[i]
		setColumns(row, fields, resultValue)
		sliceValue.Set(reflect.Append(sliceValue, resultValue.Elem()))
	}
	return nil
}

func getFieldMap(itemType reflect.Type) map[string]fieldInfo {
	fields := make(map[string]fieldInfo, itemType.NumField())
	for i := 0; i < itemType.NumField(); i++ {
		field := itemType.Field(i)
		fields[strings.ToLower(field.Name)] = fieldInfo{Name: field.Name, Index: i}
	}
	return fields
}

func setColumns(row *ldap.Entry, fields map[string]fieldInfo, result reflect.Value) {
	cols := row.Attributes
	s := result.Elem()
	for j := range cols {
		name := strings.ToLower(cols[j].Name)
		if field, ok := fields[name]; ok {
			vals := cols[j].Values
			s.Field(field.Index).Set(reflect.ValueOf(vals))
		}
	}
}

func (c *ldapBackend) QueryStructRow(query interface{}, result interface{}) error {
	if result == nil || !isPointer(reflect.TypeOf(result)) {
		return errors.New("Invalid result argument.  Must be a pointer to a struct")
	}

	res, err := c.Query(query)
	if err != nil {
		return err
	}
	resultValue := reflect.ValueOf(result) // from pointer to struct
	fields := getFieldMap(resultValue.Elem().Type())

	if len(res.Entries) == 0 {
		return errors.New("No data found")
	}
	row := res.Entries[0]
	setColumns(row, fields, resultValue)
	return nil
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
	default:
		return errInvalidLdapExecType
	}
}

func (l *ldapBackend) PasswordModify(r *ldap.PasswordModifyRequest) error {
	_, err := l.l.PasswordModify(r)
	return err
}

func (l *ldapBackend) Query(query interface{}) (*ldap.SearchResult, error) {
	q, ok := query.(*ldap.SearchRequest)
	if !ok {
		return nil, errInvalidLdapQueryType
	}
	return l.l.Search(q)
}
