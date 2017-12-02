package onedb

import (
	"gopkg.in/mgo.v2"
)

type Sessioner interface {
	DB(name string) MongoDBer
}

type MongoDBer interface {
	C(name string) MongoCollectioner
}

type MongoCollectioner interface {
	Find(query interface{}) *Query
	FindId(id interface{}) *Query
	Insert(docs ...interface{}) error
	Update(selector interface{}, update interface{}) error
	UpdateId(id interface{}, update interface{}) error
	UpdateAll(selector interface{}, update interface{}) (info *ChangeInfo, err error)
	Upsert(selector interface{}, update interface{}) (info *ChangeInfo, err error)
	UpsertId(id interface{}, update interface{}) (info *ChangeInfo, err error)
	Remove(selector interface{}) error
	RemoveId(id interface{}) error
	RemoveAll(selector interface{}) (info *ChangeInfo, err error)
}

type Session struct {
	s        *mgo.Session
	database string
}

func (s *Session) DB(name string) MongoDBer {
	return &Database{s.s.Clone().DB("name")}
}

type Database struct {
	d *mgo.Database
}

func (d *Database) C(name string) MongoCollectioner {
	return &Collection{d.d.C(name)}
}

type Collection struct {
	c *mgo.Collection
}

func testme() {
	s := &mgo.Session{}
	s.DB("string").C("name").
}
