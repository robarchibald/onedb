package mgo

import (
	"testing"
)

func TestConnect(t *testing.T) {
	s, err := Dial("localhost")
	if err != nil {
		t.Fatal(err)
	}
	t.Fatal(s.DB("edi").C("edi").Count())
}
