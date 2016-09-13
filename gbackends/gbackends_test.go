package gbackends

import (
	"os"
	"testing"
)

func TestBoltBackEnd(t *testing.T) {
	db, err := NewBoltBackEnd("test.db")
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	err1 := db.Put([]byte("carlo"), []byte("locci"))
	if err != nil {
		t.Error(err1)
	}
	err2 := db.Put([]byte("carmelo"), []byte("locci"))
	if err != nil {
		t.Error(err2)
	}
	v, err := db.Get([]byte("carlo"))
	if err != nil {
		t.Error(err)
	}
	t.Logf("value for key = carlo is : %s\n", v)
	_, err = db.Get([]byte("ca"))
	if err != ErrNotFound {
		t.Error(err)
	}

	if db.Len() != 2 {
		t.Errorf("keys are not 2, db corruptet\n")
	}
	_, err = db.Query([]byte("lo"), "p")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Query([]byte("lo"), "s")
	if err != nil {
		t.Fatal(err)
	}

	db.Del([]byte("carlo"))
	if db.Len() != 1 {
		t.Errorf("delete didn't work\n")
	}

	os.Remove("test.db")
}

func TestMapBackEnd(t *testing.T) {
	m := NewMapBackEnd()
	err := m.Put([]byte("carlo"), []byte("locci"))
	if err != nil {
		t.Fatal(err)
	}
	v, err := m.Get([]byte("carlo"))
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != "locci" {
		t.Fatal("db currpted")
	}
	_, err = m.Query([]byte("ca"), "p")
	if err == nil {
		t.Fatal(err)
	}
	_, err = m.Get([]byte("ca"))
	if err != ErrNotFound {
		t.Fatal(err)
	}
	m.Del([]byte("ca"))

	defer m.Close()

}

func TestGetBoltBackENoPermission(t *testing.T) {
	_, err := GetBoltBackEnd("/var/no.db")
	if err == nil {
		t.Error(err)
	}

}
