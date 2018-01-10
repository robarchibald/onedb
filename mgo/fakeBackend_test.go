package mgo

import (
	"fmt"
	"testing"

	"gopkg.in/mgo.v2/bson"
)

func TestNewFakeSession(t *testing.T) {
	q := interface{}(bson.M{"hello": "there"})
	r := interface{}([]string{"1", "2", "3", "4", "5"})
	fs, _ := NewFakeSession([]FakeMongoQuery{
		{DB: "db", Collection: "collection", Query: q, Return: r},
	})
	s := fs.(*fakeSession)
	var item []string
	fmt.Println(s.DB("db").C("collection").Find(q).One(&item))

}
