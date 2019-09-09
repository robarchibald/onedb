package mgo

import (
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// Dial is a light wrapper over mgo.Dial(url) function to enable mocking
func Dial(url string) (Sessioner, error) {
	s, err := mgo.Dial(url)
	if err != nil {
		return nil, err
	}
	return &msession{s}, nil
}

var (
	ErrNotFound = mgo.ErrNotFound
	ErrCursor   = mgo.ErrCursor
)

// Sessioner is the public interface for *mgo.Session to enable mocking
type Sessioner interface {
	BuildInfo() (info mgo.BuildInfo, err error)
	Clone() Sessioner
	Close()
	Copy() Sessioner
	DatabaseNames() (names []string, err error)
	DB(name string) Databaser
	EnsureSafe(safe *mgo.Safe)
	FindRef(ref *mgo.DBRef) Querier
	Fsync(async bool) error
	FsyncLock() error
	FsyncUnlock() error
	LiveServers() (addrs []string)
	Login(cred *mgo.Credential) error
	LogoutAll()
	Mode() mgo.Mode
	New() Sessioner
	Ping() error
	Refresh()
	ResetIndexCache()
	Run(cmd interface{}, result interface{}) error
	Safe() (safe *mgo.Safe)
	SelectServers(tags ...bson.D)
	SetBatch(n int)
	SetBypassValidation(bypass bool)
	SetCursorTimeout(d time.Duration)
	SetMode(consistency mgo.Mode, refresh bool)
	SetPoolLimit(limit int)
	SetPrefetch(p float64)
	SetSafe(safe *mgo.Safe)
	SetSocketTimeout(d time.Duration)
	SetSyncTimeout(d time.Duration)
}

type msession struct {
	s *mgo.Session
}

func (s *msession) BuildInfo() (info mgo.BuildInfo, err error) {
	return s.s.BuildInfo()
}
func (s *msession) Clone() Sessioner {
	return &msession{s.s.Clone()}
}
func (s *msession) Close() {
	s.s.Close()
}
func (s *msession) Copy() Sessioner {
	return &msession{s.s.Copy()}
}
func (s *msession) DatabaseNames() (names []string, err error) {
	return s.s.DatabaseNames()
}
func (s *msession) DB(name string) Databaser {
	return &mdatabase{s.s.DB(name)}
}
func (s *msession) EnsureSafe(safe *mgo.Safe) {
	s.s.EnsureSafe(safe)
}
func (s *msession) FindRef(ref *mgo.DBRef) Querier {
	return &mquery{s.s.FindRef(ref)}
}
func (s *msession) Fsync(async bool) error {
	return s.s.Fsync(async)
}
func (s *msession) FsyncLock() error {
	return s.s.FsyncLock()
}
func (s *msession) FsyncUnlock() error {
	return s.s.FsyncUnlock()
}
func (s *msession) LiveServers() (addrs []string) {
	return s.s.LiveServers()
}
func (s *msession) Login(cred *mgo.Credential) error {
	return s.s.Login(cred)
}
func (s *msession) LogoutAll() {
	s.s.LogoutAll()
}
func (s *msession) Mode() mgo.Mode {
	return s.s.Mode()
}
func (s *msession) New() Sessioner {
	s.s = s.s.New()
	return s
}
func (s *msession) Ping() error {
	return s.s.Ping()
}
func (s *msession) Refresh() {
	s.s.Refresh()
}
func (s *msession) ResetIndexCache() {
	s.s.ResetIndexCache()
}
func (s *msession) Run(cmd interface{}, result interface{}) error {
	return s.s.Run(cmd, result)
}
func (s *msession) Safe() (safe *mgo.Safe) {
	return s.s.Safe()
}
func (s *msession) SelectServers(tags ...bson.D) {
	s.s.SelectServers(tags...)
}
func (s *msession) SetBatch(n int) {
	s.s.SetBatch(n)
}
func (s *msession) SetBypassValidation(bypass bool) {
	s.s.SetBypassValidation(bypass)
}
func (s *msession) SetCursorTimeout(d time.Duration) {
	s.s.SetCursorTimeout(d)
}
func (s *msession) SetMode(consistency mgo.Mode, refresh bool) {
	s.s.SetMode(consistency, refresh)
}
func (s *msession) SetPoolLimit(limit int) {
	s.s.SetPoolLimit(limit)
}
func (s *msession) SetPrefetch(p float64) {
	s.s.SetPrefetch(p)
}
func (s *msession) SetSafe(safe *mgo.Safe) {
	s.s.SetSafe(safe)
}
func (s *msession) SetSocketTimeout(d time.Duration) {
	s.s.SetSocketTimeout(d)
}
func (s *msession) SetSyncTimeout(d time.Duration) {
	s.s.SetSyncTimeout(d)
}

// Databaser is the public interface for *mgo.Database to enable mocking
type Databaser interface {
	AddUser(username, password string, readOnly bool) error
	C(name string) Collectioner
	CollectionNames() (names []string, err error)
	DropDatabase() error
	FindRef(ref *mgo.DBRef) Querier
	GridFS(prefix string) *mgo.GridFS
	Login(user, pass string) error
	Logout()
	RemoveUser(user string) error
	Run(cmd interface{}, result interface{}) error
	UpsertUser(user *mgo.User) error
	With(s *mgo.Session) Databaser
}

type mdatabase struct {
	d *mgo.Database
}

func (d *mdatabase) AddUser(username, password string, readOnly bool) error {
	return d.d.AddUser(username, password, readOnly)
}
func (d *mdatabase) C(name string) Collectioner {
	return &mcollection{d.d.C(name)}
}
func (d *mdatabase) CollectionNames() (names []string, err error) {
	return d.d.CollectionNames()
}
func (d *mdatabase) DropDatabase() error {
	return d.d.DropDatabase()
}
func (d *mdatabase) FindRef(ref *mgo.DBRef) Querier {
	return &mquery{d.d.FindRef(ref)}
}
func (d *mdatabase) GridFS(prefix string) *mgo.GridFS {
	return d.d.GridFS(prefix)
}
func (d *mdatabase) Login(user, pass string) error {
	return d.d.Login(user, pass)
}
func (d *mdatabase) Logout() {
	d.d.Logout()
}
func (d *mdatabase) RemoveUser(user string) error {
	return d.d.RemoveUser(user)
}
func (d *mdatabase) Run(cmd interface{}, result interface{}) error {
	return d.d.Run(cmd, result)
}
func (d *mdatabase) UpsertUser(user *mgo.User) error {
	return d.d.UpsertUser(user)
}
func (d *mdatabase) With(s *mgo.Session) Databaser {
	return &mdatabase{d.d.With(s)}
}

// Collectioner is the public interface for *mgo.Collection to enable mocking
type Collectioner interface {
	Count() (n int, err error)
	Create(info *mgo.CollectionInfo) error
	DropCollection() error
	DropIndex(key ...string) error
	DropIndexName(name string) error
	EnsureIndex(index mgo.Index) error
	EnsureIndexKey(key ...string) error
	Find(query interface{}) Querier
	FindId(id interface{}) Querier
	Indexes() (indexes []mgo.Index, err error)
	Insert(docs ...interface{}) error
	NewIter(session *mgo.Session, firstBatch []bson.Raw, cursorId int64, err error) Iterator
	Pipe(pipeline interface{}) *mgo.Pipe
	Remove(selector interface{}) error
	RemoveId(id interface{}) error
	RemoveAll(selector interface{}) (info *mgo.ChangeInfo, err error)
	Repair() Iterator
	Update(selector interface{}, update interface{}) error
	UpdateId(id interface{}, update interface{}) error
	UpdateAll(selector interface{}, update interface{}) (info *mgo.ChangeInfo, err error)
	Upsert(selector interface{}, update interface{}) (info *mgo.ChangeInfo, err error)
	UpsertId(id interface{}, update interface{}) (info *mgo.ChangeInfo, err error)
	With(s *mgo.Session) Collectioner

	MethodCalls() []methodCall
}

type mcollection struct {
	c *mgo.Collection
}

func (c *mcollection) Count() (n int, err error) {
	return c.c.Count()
}
func (c *mcollection) Create(info *mgo.CollectionInfo) error {
	return c.c.Create(info)
}
func (c *mcollection) DropCollection() error {
	return c.c.DropCollection()
}
func (c *mcollection) DropIndex(key ...string) error {
	return c.c.DropIndex(key...)
}
func (c *mcollection) DropIndexName(name string) error {
	return c.c.DropIndexName(name)
}
func (c *mcollection) EnsureIndex(index mgo.Index) error {
	return c.c.EnsureIndex(index)
}
func (c *mcollection) EnsureIndexKey(key ...string) error {
	return c.c.EnsureIndexKey(key...)
}
func (c *mcollection) Find(query interface{}) Querier {
	return &mquery{c.c.Find(query)}
}
func (c *mcollection) FindId(id interface{}) Querier {
	return &mquery{c.c.FindId(id)}
}
func (c *mcollection) Insert(docs ...interface{}) error {
	return c.c.Insert(docs...)
}
func (c *mcollection) Update(selector interface{}, update interface{}) error {
	return c.c.Update(selector, update)
}
func (c *mcollection) UpdateId(id interface{}, update interface{}) error {
	return c.c.UpdateId(id, update)
}
func (c *mcollection) UpdateAll(selector interface{}, update interface{}) (info *mgo.ChangeInfo, err error) {
	return c.c.UpdateAll(selector, update)
}
func (c *mcollection) Upsert(selector interface{}, update interface{}) (info *mgo.ChangeInfo, err error) {
	return c.c.Upsert(selector, update)
}
func (c *mcollection) UpsertId(id interface{}, update interface{}) (info *mgo.ChangeInfo, err error) {
	return c.c.UpsertId(id, update)
}
func (c *mcollection) Remove(selector interface{}) error {
	return c.c.Remove(selector)
}
func (c *mcollection) RemoveId(id interface{}) error {
	return c.c.RemoveId(id)
}
func (c *mcollection) RemoveAll(selector interface{}) (info *mgo.ChangeInfo, err error) {
	return c.c.RemoveAll(selector)
}
func (c *mcollection) Indexes() (indexes []mgo.Index, err error) {
	return c.c.Indexes()
}
func (c *mcollection) NewIter(session *mgo.Session, firstBatch []bson.Raw, cursorId int64, err error) Iterator {
	return c.c.NewIter(session, firstBatch, cursorId, err)
}
func (c *mcollection) Pipe(pipeline interface{}) *mgo.Pipe {
	return c.c.Pipe(pipeline)
}
func (c *mcollection) Repair() Iterator {
	return c.c.Repair()
}
func (c *mcollection) With(s *mgo.Session) Collectioner {
	return &mcollection{c.c.With(s)}
}
func (c *mcollection) MethodCalls() []methodCall {
	return nil
}

// Querier is the public interface for *mgo.Query to enable mocking
type Querier interface {
	All(result interface{}) error
	Apply(change mgo.Change, result interface{}) (info *mgo.ChangeInfo, err error)
	Batch(n int) Querier
	Comment(comment string) Querier
	Count() (n int, err error)
	Distinct(key string, result interface{}) error
	Explain(result interface{}) error
	Hint(indexKey ...string) Querier
	Iter() Iterator
	Limit(n int) Querier
	LogReplay() Querier
	MapReduce(job *mgo.MapReduce, result interface{}) (info *mgo.MapReduceInfo, err error)
	One(result interface{}) (err error)
	Prefetch(p float64) Querier
	Select(selector interface{}) Querier
	SetMaxScan(n int) Querier
	SetMaxTime(d time.Duration) Querier
	Snapshot() Querier
	Sort(fields ...string) Querier
	Skip(n int) Querier
	Tail(timeout time.Duration) Iterator
}

type mquery struct {
	q *mgo.Query
}

func (q *mquery) All(result interface{}) error {
	return q.q.All(result)
}
func (q *mquery) Apply(change mgo.Change, result interface{}) (info *mgo.ChangeInfo, err error) {
	return q.q.Apply(change, result)
}
func (q *mquery) Batch(n int) Querier {
	q.q = q.q.Batch(n)
	return q
}
func (q *mquery) Comment(comment string) Querier {
	q.q = q.q.Comment(comment)
	return q
}
func (q *mquery) Count() (n int, err error) {
	return q.q.Count()
}
func (q *mquery) Distinct(key string, result interface{}) error {
	return q.q.Distinct(key, result)
}
func (q *mquery) Explain(result interface{}) error {
	return q.q.Explain(result)
}
func (q *mquery) Hint(indexKey ...string) Querier {
	q.q = q.q.Hint(indexKey...)
	return q
}
func (q *mquery) Iter() Iterator {
	return q.q.Iter()
}
func (q *mquery) Limit(n int) Querier {
	q.q = q.q.Limit(n)
	return q
}
func (q *mquery) LogReplay() Querier {
	q.q = q.q.LogReplay()
	return q
}
func (q *mquery) MapReduce(job *mgo.MapReduce, result interface{}) (info *mgo.MapReduceInfo, err error) {
	return q.q.MapReduce(job, result)
}
func (q *mquery) One(result interface{}) (err error) {
	return q.q.One(result)
}
func (q *mquery) Prefetch(p float64) Querier {
	q.q = q.q.Prefetch(p)
	return q
}
func (q *mquery) Select(selector interface{}) Querier {
	q.q = q.q.Select(selector)
	return q
}
func (q *mquery) SetMaxScan(n int) Querier {
	q.q = q.q.SetMaxScan(n)
	return q
}
func (q *mquery) SetMaxTime(d time.Duration) Querier {
	q.q = q.q.SetMaxTime(d)
	return q
}
func (q *mquery) Snapshot() Querier {
	q.q = q.q.Snapshot()
	return q
}
func (q *mquery) Sort(fields ...string) Querier {
	q.q = q.q.Sort(fields...)
	return q
}
func (q *mquery) Skip(n int) Querier {
	q.q = q.q.Skip(n)
	return q
}
func (q *mquery) Tail(timeout time.Duration) Iterator {
	return q.q.Tail(timeout)
}

// Iterator is the public interface for *mgo.Iter to enable mocking
type Iterator interface {
	Err() error
	Done() bool
	Close() error
	Next(result interface{}) bool
	All(result interface{}) error
}

// Piper is the public interface for *mgo.Pipe to enable mocking
type Piper interface {
	AllowDiskUse() Piper
	Batch(n int) Piper
	Iter() Iterator
	All(result interface{}) error
	One(result interface{}) error
	Explain(result interface{}) error
}
