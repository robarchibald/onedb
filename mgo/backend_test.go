package mgo

import (
	"testing"
)

func TestConnect(t *testing.T) {
	s, err := Dial("localhost")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB("edi").C("edi").Count(); err != nil {
		t.Fatal(err)
	}
}
