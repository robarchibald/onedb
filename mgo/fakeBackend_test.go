package mgo

import (
	"testing"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

func TestNewFakeSession(t *testing.T) {
	q := interface{}(bson.M{"hello": "there"})
	r := interface{}([]string{"1", "2", "3", "4", "5"})
	fs, _ := NewFakeSession([]FakeMongoQuery{
		{DB: "db", Collection: "collection", Query: q, Return: r},
	})
	var item []string
	if err := fs.DB("db").C("collection").Find(q).One(&item); err != nil {
		t.Error(err)
	}
	if len(item) != 5 || item[0] != "1" || item[1] != "2" || item[2] != "3" || item[3] != "4" || item[4] != "5" {
		t.Error("Expected correct values")
	}
}

func TestQueriesRun(t *testing.T) {
	fs, _ := NewFakeSession(nil)
	c := fs.DB("db").C("collection")
	c.Count()
	c.Create(nil)
	c.DropCollection()
	c.DropIndex("key")
	c.DropIndexName("name")
	c.EnsureIndex(mgo.Index{})
	c.EnsureIndexKey("key")
	c.Find(nil)
	c.FindId("")
	c.Indexes()
	c.Insert()
	c.NewIter(nil, nil, 1, nil)
	c.Pipe(nil)
	c.Remove("")
	c.RemoveAll("")
	c.RemoveId("")
	c.Repair()
	c.Update("", "")
	c.UpdateAll("", "")
	c.UpdateId("", "")
	c.Upsert("", "")
	c.UpsertId("", "")
	c.With(nil)
	if l := len(c.MethodCalls()); l != 23 {
		t.Error("Expected queries to be logged", l)
	}
}
