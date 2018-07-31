package mgo

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type sessionToDBMap map[string]*fakeDatabase
type dbToCollectionMap map[string]*fakeCollection

// FakeMongoQuery is a struct which holds return values for queries
type FakeMongoQuery struct {
	DB         string
	Collection string
	Query      interface{}
	Return     interface{}
}

type query struct {
	Query  interface{}
	Return interface{}
}

// NewFakeSession creates a fake mgo.Sessioner for mocking purposes
func NewFakeSession(queryResults []FakeMongoQuery) (Sessioner, error) {
	smap := make(sessionToDBMap)
	for i := range queryResults {
		r := queryResults[i]
		d := getDatabase(smap, r.DB)
		c := getCollection(d, r.Collection)
		c.q = append(c.q, query{r.Query, r.Return})
		fmt.Println(smap, c.q)
	}
	return &fakeSession{smap}, nil
}

func getDatabase(smap sessionToDBMap, dbName string) *fakeDatabase {
	d, ok := smap[dbName]
	if !ok {
		d = &fakeDatabase{make(dbToCollectionMap)}
		smap[dbName] = d
	}
	return d
}

func getCollection(db *fakeDatabase, collectionName string) *fakeCollection {
	c, ok := db.collections[collectionName]
	if !ok {
		c = &fakeCollection{}
		db.collections[collectionName] = c
	}
	return c
}

type fakeSession struct {
	data sessionToDBMap
}

func (s *fakeSession) BuildInfo() (info mgo.BuildInfo, err error) { return mgo.BuildInfo{}, nil }
func (s *fakeSession) Clone() Sessioner                           { return s }
func (s *fakeSession) Close()                                     {}
func (s *fakeSession) Copy() Sessioner                            { return s }
func (s *fakeSession) DatabaseNames() (names []string, err error) {
	var n []string
	for key := range s.data {
		n = append(n, key)
	}
	return n, nil
}
func (s *fakeSession) DB(name string) Databaser {
	db, ok := s.data[name]
	if ok {
		return db
	}
	return &fakeDatabase{make(dbToCollectionMap)}
}
func (s *fakeSession) EnsureSafe(safe *mgo.Safe) {}
func (s *fakeSession) FindRef(ref *mgo.DBRef) Querier {
	return s.DB(ref.Database).C(ref.Collection).FindId(ref.Id)
}
func (s *fakeSession) Fsync(async bool) error                        { return nil }
func (s *fakeSession) FsyncLock() error                              { return nil }
func (s *fakeSession) FsyncUnlock() error                            { return nil }
func (s *fakeSession) LiveServers() (addrs []string)                 { return nil }
func (s *fakeSession) Login(cred *mgo.Credential) error              { return nil }
func (s *fakeSession) LogoutAll()                                    {}
func (s *fakeSession) Mode() mgo.Mode                                { return mgo.Primary }
func (s *fakeSession) New() Sessioner                                { return s }
func (s *fakeSession) Ping() error                                   { return nil }
func (s *fakeSession) Refresh()                                      {}
func (s *fakeSession) ResetIndexCache()                              {}
func (s *fakeSession) Run(cmd interface{}, result interface{}) error { return nil }
func (s *fakeSession) Safe() (safe *mgo.Safe)                        { return nil }
func (s *fakeSession) SelectServers(tags ...bson.D)                  {}
func (s *fakeSession) SetBatch(n int)                                {}
func (s *fakeSession) SetBypassValidation(bypass bool)               {}
func (s *fakeSession) SetCursorTimeout(d time.Duration)              {}
func (s *fakeSession) SetMode(consistency mgo.Mode, refresh bool)    {}
func (s *fakeSession) SetPoolLimit(limit int)                        {}
func (s *fakeSession) SetPrefetch(p float64)                         {}
func (s *fakeSession) SetSafe(safe *mgo.Safe)                        {}
func (s *fakeSession) SetSocketTimeout(d time.Duration)              {}
func (s *fakeSession) SetSyncTimeout(d time.Duration)                {}

type fakeDatabase struct {
	collections dbToCollectionMap
}

func (d *fakeDatabase) AddUser(username, password string, readOnly bool) error { return nil }
func (d *fakeDatabase) C(name string) Collectioner {
	c, ok := d.collections[name]
	if ok {
		return c
	}
	return &fakeCollection{[]query{}, []methodCall{}}
}
func (d *fakeDatabase) CollectionNames() (names []string, err error) {
	var n []string
	for key := range d.collections {
		n = append(n, key)
	}
	return n, nil
}
func (d *fakeDatabase) DropDatabase() error { return nil }
func (d *fakeDatabase) FindRef(ref *mgo.DBRef) Querier {
	return d.C(ref.Collection).FindId(ref.Id)
}
func (d *fakeDatabase) GridFS(prefix string) *mgo.GridFS              { return &mgo.GridFS{} }
func (d *fakeDatabase) Login(user, pass string) error                 { return nil }
func (d *fakeDatabase) Logout()                                       {}
func (d *fakeDatabase) RemoveUser(user string) error                  { return nil }
func (d *fakeDatabase) Run(cmd interface{}, result interface{}) error { return nil }
func (d *fakeDatabase) UpsertUser(user *mgo.User) error               { return nil }
func (d *fakeDatabase) With(s *mgo.Session) Databaser                 { return d }

type methodCall struct {
	Name string
	Args []interface{}
}

func newMethodCall(name string, args ...interface{}) *methodCall {
	return &methodCall{Name: name, Args: args}
}

type fakeCollection struct {
	q             []query
	methodsCalled []methodCall
}

func (c *fakeCollection) Count() (n int, err error) {
	c.methodsCalled = append(c.methodsCalled, *newMethodCall("Count"))
	return -1, nil
}
func (c *fakeCollection) Create(info *mgo.CollectionInfo) error {
	c.methodsCalled = append(c.methodsCalled, *newMethodCall("Create", info))
	return nil
}
func (c *fakeCollection) DropCollection() error {
	c.methodsCalled = append(c.methodsCalled, *newMethodCall("DropCollection"))
	return nil
}
func (c *fakeCollection) DropIndex(key ...string) error {
	c.methodsCalled = append(c.methodsCalled, *newMethodCall("DropIndex", key))
	return nil
}
func (c *fakeCollection) DropIndexName(name string) error {
	c.methodsCalled = append(c.methodsCalled, *newMethodCall("DropIndexName", name))
	return nil
}
func (c *fakeCollection) EnsureIndex(index mgo.Index) error {
	c.methodsCalled = append(c.methodsCalled, *newMethodCall("EnsureIndex", index))
	return nil
}
func (c *fakeCollection) EnsureIndexKey(key ...string) error {
	c.methodsCalled = append(c.methodsCalled, *newMethodCall("EnsureIndexKey", key))
	return nil
}
func (c *fakeCollection) find(query interface{}) Querier {
	for i := range c.q {
		if reflect.DeepEqual(c.q[i].Query, query) {
			return &fakeQuery{c.q[i].Return}
		}
	}
	return &fakeQuery{}
}
func (c *fakeCollection) Find(query interface{}) Querier {
	c.methodsCalled = append(c.methodsCalled, *newMethodCall("Find", query))
	return c.find(query)
}
func (c *fakeCollection) FindId(id interface{}) Querier {
	c.methodsCalled = append(c.methodsCalled, *newMethodCall("FindId", id))
	return c.find(id)
}
func (c *fakeCollection) Insert(docs ...interface{}) error {
	c.methodsCalled = append(c.methodsCalled, *newMethodCall("Insert", docs))
	return nil
}
func (c *fakeCollection) Update(selector interface{}, update interface{}) error {
	c.methodsCalled = append(c.methodsCalled, *newMethodCall("Update", selector, update))
	return nil
}
func (c *fakeCollection) UpdateId(id interface{}, update interface{}) error {
	c.methodsCalled = append(c.methodsCalled, *newMethodCall("UpdateId", id, update))
	return nil
}
func (c *fakeCollection) UpdateAll(selector interface{}, update interface{}) (info *mgo.ChangeInfo, err error) {
	c.methodsCalled = append(c.methodsCalled, *newMethodCall("UpdateAll", selector, update))
	return nil, nil
}
func (c *fakeCollection) Upsert(selector interface{}, update interface{}) (info *mgo.ChangeInfo, err error) {
	c.methodsCalled = append(c.methodsCalled, *newMethodCall("Upsert", selector, update))
	return nil, nil
}
func (c *fakeCollection) UpsertId(id interface{}, update interface{}) (info *mgo.ChangeInfo, err error) {
	c.methodsCalled = append(c.methodsCalled, *newMethodCall("UpsertId", id, update))
	return nil, nil
}
func (c *fakeCollection) Remove(selector interface{}) error {
	c.methodsCalled = append(c.methodsCalled, *newMethodCall("Remove", selector))
	return nil
}
func (c *fakeCollection) RemoveId(id interface{}) error {
	c.methodsCalled = append(c.methodsCalled, *newMethodCall("RemoveId", id))
	return nil
}
func (c *fakeCollection) RemoveAll(selector interface{}) (info *mgo.ChangeInfo, err error) {
	c.methodsCalled = append(c.methodsCalled, *newMethodCall("RemoveAll", selector))
	return nil, nil
}
func (c *fakeCollection) Indexes() (indexes []mgo.Index, err error) {
	c.methodsCalled = append(c.methodsCalled, *newMethodCall("Indexes"))
	return nil, nil
}
func (c *fakeCollection) NewIter(session *mgo.Session, firstBatch []bson.Raw, cursorId int64, err error) Iterator {
	c.methodsCalled = append(c.methodsCalled, *newMethodCall("NewIter", session, firstBatch, cursorId, err))
	return nil
}
func (c *fakeCollection) Pipe(pipeline interface{}) *mgo.Pipe {
	c.methodsCalled = append(c.methodsCalled, *newMethodCall("Pipe", pipeline))
	return nil
}
func (c *fakeCollection) Repair() Iterator {
	c.methodsCalled = append(c.methodsCalled, *newMethodCall("Repair"))
	return nil
}
func (c *fakeCollection) With(s *mgo.Session) Collectioner {
	c.methodsCalled = append(c.methodsCalled, *newMethodCall("With", s))
	return c
}

type fakeQuery struct {
	r interface{}
}

func (q *fakeQuery) All(result interface{}) error {
	if q.r == nil {
		return ErrNotFound
	}
	return convertAssign(result, q.r)
}
func (q *fakeQuery) Apply(change mgo.Change, result interface{}) (info *mgo.ChangeInfo, err error) {
	return nil, nil
}
func (q *fakeQuery) Batch(n int) Querier            { return q }
func (q *fakeQuery) Comment(comment string) Querier { return q }
func (q *fakeQuery) Count() (n int, err error)      { return -1, nil }
func (q *fakeQuery) Distinct(key string, result interface{}) error {
	return q.All(result)
}
func (q *fakeQuery) Explain(result interface{}) error { return q.One(result) }
func (q *fakeQuery) Hint(indexKey ...string) Querier  { return q }
func (q *fakeQuery) Iter() Iterator                   { return nil }
func (q *fakeQuery) Limit(n int) Querier              { return q }
func (q *fakeQuery) LogReplay() Querier               { return q }
func (q *fakeQuery) MapReduce(job *mgo.MapReduce, result interface{}) (info *mgo.MapReduceInfo, err error) {
	return nil, q.All(result)
}
func (q *fakeQuery) One(result interface{}) (err error)  { return q.All(result) }
func (q *fakeQuery) Prefetch(p float64) Querier          { return q }
func (q *fakeQuery) Select(selector interface{}) Querier { return q }
func (q *fakeQuery) SetMaxScan(n int) Querier            { return q }
func (q *fakeQuery) SetMaxTime(d time.Duration) Querier  { return q }
func (q *fakeQuery) Snapshot() Querier                   { return q }
func (q *fakeQuery) Sort(fields ...string) Querier       { return q }
func (q *fakeQuery) Skip(n int) Querier                  { return q }
func (q *fakeQuery) Tail(timeout time.Duration) Iterator { return nil }

var errNilPtr = errors.New("destination pointer is nil") // embedded in descriptive error

// convertAssign copies to dest the value in src, converting it if possible.
// An error is returned if the copy would result in loss of information.
// dest should be a pointer type.
func convertAssign(dest, src interface{}) error {
	// Common cases, without reflect.
	switch s := src.(type) {
	case string:
		switch d := dest.(type) {
		case *string:
			if d == nil {
				return errNilPtr
			}
			*d = s
			return nil
		case *[]byte:
			if d == nil {
				return errNilPtr
			}
			*d = []byte(s)
			return nil
		}
	case []byte:
		switch d := dest.(type) {
		case *string:
			if d == nil {
				return errNilPtr
			}
			*d = string(s)
			return nil
		case *interface{}:
			if d == nil {
				return errNilPtr
			}
			*d = cloneBytes(s)
			return nil
		case *[]byte:
			if d == nil {
				return errNilPtr
			}
			*d = cloneBytes(s)
			return nil
		}
	case time.Time:
		switch d := dest.(type) {
		case *time.Time:
			*d = s
			return nil
		case *string:
			*d = s.Format(time.RFC3339Nano)
			return nil
		case *[]byte:
			if d == nil {
				return errNilPtr
			}
			*d = []byte(s.Format(time.RFC3339Nano))
			return nil
		}
	case nil:
		switch d := dest.(type) {
		case *interface{}:
			if d == nil {
				return errNilPtr
			}
			*d = nil
			return nil
		case *[]byte:
			if d == nil {
				return errNilPtr
			}
			*d = nil
			return nil
		}
	}

	var sv reflect.Value

	switch d := dest.(type) {
	case *string:
		sv = reflect.ValueOf(src)
		switch sv.Kind() {
		case reflect.Bool,
			reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.Float32, reflect.Float64:
			*d = asString(src)
			return nil
		}
	case *[]byte:
		sv = reflect.ValueOf(src)
		if b, ok := asBytes(nil, sv); ok {
			*d = b
			return nil
		}
	case *bool:
		bv, err := convertBool(src)
		if err == nil {
			*d = bv.(bool)
		}
		return err
	case *interface{}:
		*d = src
		return nil
	}

	dpv := reflect.ValueOf(dest)
	if dpv.Kind() != reflect.Ptr {
		return errors.New("destination not a pointer")
	}
	if dpv.IsNil() {
		return errNilPtr
	}

	if !sv.IsValid() {
		sv = reflect.ValueOf(src)
	}

	dv := reflect.Indirect(dpv)
	if sv.IsValid() && sv.Type().AssignableTo(dv.Type()) {
		switch b := src.(type) {
		case []byte:
			dv.Set(reflect.ValueOf(cloneBytes(b)))
		default:
			dv.Set(sv)
		}
		return nil
	}

	if dv.Kind() == sv.Kind() && sv.Type().ConvertibleTo(dv.Type()) {
		dv.Set(sv.Convert(dv.Type()))
		return nil
	}

	// The following conversions use a string value as an intermediate representation
	// to convert between various numeric types.
	//
	// This also allows scanning into user defined types such as "type Int int64".
	// For symmetry, also check for string destination types.
	switch dv.Kind() {
	case reflect.Ptr:
		if src == nil {
			dv.Set(reflect.Zero(dv.Type()))
			return nil
		} else {
			dv.Set(reflect.New(dv.Type().Elem()))
			return convertAssign(dv.Interface(), src)
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		s := asString(src)
		i64, err := strconv.ParseInt(s, 10, dv.Type().Bits())
		if err != nil {
			err = strconvErr(err)
			return fmt.Errorf("converting driver.Value type %T (%q) to a %s: %v", src, s, dv.Kind(), err)
		}
		dv.SetInt(i64)
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		s := asString(src)
		u64, err := strconv.ParseUint(s, 10, dv.Type().Bits())
		if err != nil {
			err = strconvErr(err)
			return fmt.Errorf("converting driver.Value type %T (%q) to a %s: %v", src, s, dv.Kind(), err)
		}
		dv.SetUint(u64)
		return nil
	case reflect.Float32, reflect.Float64:
		s := asString(src)
		f64, err := strconv.ParseFloat(s, dv.Type().Bits())
		if err != nil {
			err = strconvErr(err)
			return fmt.Errorf("converting driver.Value type %T (%q) to a %s: %v", src, s, dv.Kind(), err)
		}
		dv.SetFloat(f64)
		return nil
	case reflect.String:
		switch v := src.(type) {
		case string:
			dv.SetString(v)
			return nil
		case []byte:
			dv.SetString(string(v))
			return nil
		}
	}

	return fmt.Errorf("unsupported Scan, storing driver.Value type %T into type %T", src, dest)
}

func strconvErr(err error) error {
	if ne, ok := err.(*strconv.NumError); ok {
		return ne.Err
	}
	return err
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	} else {
		c := make([]byte, len(b))
		copy(c, b)
		return c
	}
}

func asString(src interface{}) string {
	switch v := src.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	}
	rv := reflect.ValueOf(src)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(rv.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(rv.Uint(), 10)
	case reflect.Float64:
		return strconv.FormatFloat(rv.Float(), 'g', -1, 64)
	case reflect.Float32:
		return strconv.FormatFloat(rv.Float(), 'g', -1, 32)
	case reflect.Bool:
		return strconv.FormatBool(rv.Bool())
	}
	return fmt.Sprintf("%v", src)
}

func asBytes(buf []byte, rv reflect.Value) (b []byte, ok bool) {
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.AppendInt(buf, rv.Int(), 10), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.AppendUint(buf, rv.Uint(), 10), true
	case reflect.Float32:
		return strconv.AppendFloat(buf, rv.Float(), 'g', -1, 32), true
	case reflect.Float64:
		return strconv.AppendFloat(buf, rv.Float(), 'g', -1, 64), true
	case reflect.Bool:
		return strconv.AppendBool(buf, rv.Bool()), true
	case reflect.String:
		s := rv.String()
		return append(buf, s...), true
	}
	return
}

func convertBool(src interface{}) (interface{}, error) {
	switch s := src.(type) {
	case bool:
		return s, nil
	case string:
		b, err := strconv.ParseBool(s)
		if err != nil {
			return nil, fmt.Errorf("sql/driver: couldn't convert %q into type bool", s)
		}
		return b, nil
	case []byte:
		b, err := strconv.ParseBool(string(s))
		if err != nil {
			return nil, fmt.Errorf("sql/driver: couldn't convert %q into type bool", s)
		}
		return b, nil
	}

	sv := reflect.ValueOf(src)
	switch sv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		iv := sv.Int()
		if iv == 1 || iv == 0 {
			return iv == 1, nil
		}
		return nil, fmt.Errorf("sql/driver: couldn't convert %d into type bool", iv)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		uv := sv.Uint()
		if uv == 1 || uv == 0 {
			return uv == 1, nil
		}
		return nil, fmt.Errorf("sql/driver: couldn't convert %d into type bool", uv)
	}

	return nil, fmt.Errorf("sql/driver: couldn't convert %v (%T) into type bool", src, src)
}
