package onedb

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"gopkg.in/ldap.v2"
	"math"
	"net"
	"reflect"
	"strings"
	"time"
)

var errInvalidLdapQueryType = errors.New("Invalid query. Must be of type *ldap.SearchRequest")
var errInvalidLdapExecType = errors.New("Invalid execute request. Must be of type *ldap.AddRequest, *ldap.DelRequest, *ldap.ModifyRequest or *ldap.PasswordModifyRequest")
var ldapCreate ldapCreator = &ldapRealCreator{}

type ldapCreator interface {
	NewConn(conn net.Conn, isTLS bool) ldapBackender
}

type ldapRealCreator struct{}

func (d *ldapRealCreator) NewConn(conn net.Conn, isTLS bool) ldapBackender {
	return ldap.NewConn(conn, isTLS)
}

type ldapBackend struct {
	l          ldapBackender
	lastRetry  time.Time
	retryCount int
	hostname   string
	port       int
	binddn     string
	password   string
}

type ldapBackender interface {
	Start()
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
	l, err := ldapConnect(hostname, port, binddn, password)
	if err != nil {
		return nil, err
	}
	return &ldapBackend{l: l, hostname: hostname, port: port, binddn: binddn, password: password}, nil
}

func ldapConnect(hostname string, port int, binddn string, password string) (ldapBackender, error) {
	tc, err := dialHelper.Dial("tcp", fmt.Sprintf("%s:%d", hostname, port))
	if err != nil {
		return nil, err
	}
	l := ldapCreate.NewConn(tc, false)
	l.Start()
	if err = l.StartTLS(&tls.Config{ServerName: hostname}); err != nil {
		return nil, err
	}

	if err := l.Bind(binddn, password); err != nil {
		return nil, err
	}

	return l, nil
}

func (c *ldapBackend) Bind(username, password string) error {
	return c.l.Bind(username, password)
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
	Kind  reflect.Kind
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
		fields[strings.ToLower(field.Name)] = fieldInfo{Name: field.Name, Index: i, Kind: field.Type.Kind()}
	}
	return fields
}

func setColumns(row *ldap.Entry, fields map[string]fieldInfo, result reflect.Value) error {
	cols := row.Attributes
	s := result.Elem()
	for j := range cols {
		name := strings.ToLower(cols[j].Name)
		if field, ok := fields[name]; ok {
			if err := setRowValue(s, &field, cols[j].Values); err != nil {
				return err
			}
		}
	}
	return nil
}

func setRowValue(row reflect.Value, field *fieldInfo, vals []string) error {
	if field.Kind == reflect.Slice {
		row.Field(field.Index).Set(reflect.ValueOf(vals))
	} else if len(vals) == 1 {
		row.Field(field.Index).Set(reflect.ValueOf(vals[0]))
	} else if len(vals) > 1 {
		return fmt.Errorf("Expected single value for field: %s, but found %d", field.Name, len(vals))
	}
	return nil
}

func (c *ldapBackend) QueryValues(query interface{}, result ...interface{}) error {
	if result == nil || !isPointer(reflect.TypeOf(result)) || reflect.TypeOf(result).Elem().Kind() == reflect.Struct {
		return errors.New("Invalid result argument.  Must be a pointer to a primitive type")
	}

	res, err := c.Query(query)
	if err != nil {
		return err
	}

	if len(res.Entries) != 1 || len(res.Entries[0].Attributes) != len(result) {
		return errors.Errorf("Expected 1 row and %d column of data. Found %d row(s) and %d column(s)", len(result), len(res.Entries), len(res.Entries[0].Attributes))
	}
	for i := 0; i < len(result); i++ {
		setRowValue(reflect.ValueOf(result[i]), &fieldInfo{Kind: reflect.Invalid}, res.Entries[0].Attributes[i].Values)
	}
	reflect.ValueOf(result).Set(reflect.ValueOf(res.Entries[0].Attributes[0].Values[0]))
	return nil
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
	var err error
	switch r := query.(type) {
	case *ldap.AddRequest:
		err = l.l.Add(r)
	case *ldap.DelRequest:
		err = l.l.Del(r)
	case *ldap.ModifyRequest:
		err = l.l.Modify(r)
	case *ldap.PasswordModifyRequest:
		_, err = l.l.PasswordModify(r)
	case *ldap.SimpleBindRequest:
		err = l.l.Bind(r.Username, r.Password)
	default:
		err = errInvalidLdapExecType
	}
	if err != nil && err.Error() == "ldap: connection closed" && l.reconnect() {
		return l.Execute(query)
	}
	return err
}

func (l *ldapBackend) Query(query interface{}) (*ldap.SearchResult, error) {
	q, ok := query.(*ldap.SearchRequest)
	if !ok {
		return nil, errInvalidLdapQueryType
	}
	res, err := l.l.Search(q)
	if err != nil && err.Error() == "ldap: connection closed" && l.reconnect() {
		return l.Query(query)
	}
	return res, err
}

func (l *ldapBackend) reconnect() bool {
	var err error
	ms := time.Millisecond * time.Duration(math.Pow10(l.retryCount)) // retry every 10^lastRetry milliseconds
	if time.Since(l.lastRetry) > ms {
		l.l, err = ldapConnect(l.hostname, l.port, l.binddn, l.password)
		if err == nil {
			l.retryCount = 0
		} else if l.retryCount <= 4 { // max retry time is 10 seconds
			l.retryCount++
		}
		l.lastRetry = time.Now()
		return true
	}
	return false
}
