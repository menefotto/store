package store

import (
	"bytes"
	"compress/gzip"
	"os"
	"testing"

	"github.com/sonic/lib/store/gbackends"
)

func TestStore(t *testing.T) {
	var db gbackends.BoltBackEnd
	err := db.Open("test.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.Put([]byte("stovari"), []byte("miao"))

	g := New(&db)
	_ = g.Add("carlo", []byte("ciao"))

	_, err = g.Get("stovari")
	if err == nil {
		t.Fatal(err)
	}
	_, err = g.Get("storie")
	if err == nil {
		t.Fatal(err)
	}

	_ = g.BackEnd()
	g.Close()
	os.Remove("test.db")
}

func TestStoreAddDelGet(t *testing.T) {
	var db gbackends.BoltBackEnd
	err := db.Open("test.db")
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	g := New(&db)
	_ = g.Add("carlo", []byte("ciao"))

	_, err = g.Get("carlo")
	if err != nil {
		t.Errorf("Error in Get %v\n", err)
	}

	g.Del("carlo")
	if db.Len() != 0 {
		t.Error("Failed to delete the key/value")
	}

	err = PrettyPrint(db)
	if err == nil {
		t.Error("should have not printed")
	}
	os.Remove("test.db")
}

func TestStoreMemory(t *testing.T) {
	db := gbackends.NewMapBackEnd()
	g := New(db)
	defer g.Close()
	_ = g.Add("carlo", []byte("locci"))

	val, err := g.Get("carlo")
	if err != nil {
		t.Errorf("Error in Get %v\n", err)
	}

	var s []byte
	_, err = val.Data(&s)
	if err != nil {
		t.Fatal(err)
	}

	if string(s) != "locci" {
		t.Errorf("they should be equal %s, %s\n", s, "locci")
	}

	err = MarshallToDisk(db)
	if err != nil {
		t.Fatal(err)
	}

	dbnew := gbackends.NewMapBackEnd()
	err = UnmarshalFromDisk(dbnew)
	if err != nil {
		t.Fatal(err)
	}
	err = UnmarshalFromDisk("test")
	if err == nil {
		t.Fatal("shoul not be possible to unmarshall a string")
	}
	err = MarshallToDisk("test")
	if err == nil {
		t.Fatal("shoul not be possible to unmarshall a string")
	}
	err = PrettyPrint(db)
	if err != nil {
		t.Error(err)
	}

}

func TestAny(t *testing.T) {
	var empty string = "XXXXXXX"
	gz := gzip.NewWriter(bytes.NewBuffer([]byte(empty)))

	a := NewAny(nil)
	a.Data(gz)
}
