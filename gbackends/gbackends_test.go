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

	if db.Len() != 2 {
		t.Errorf("keys are not 2, db corruptet\n")
	}

	db.Del([]byte("carlo"))
	if db.Len() != 1 {
		t.Errorf("delete didn't work\n")
	}

	os.Remove("test.db")
}

func TestMapBackEnd(t *testing.T) {
	_ = NewMapBackEnd()
}
