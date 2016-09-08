package graph

import (
	"fmt"
	"os"
	"testing"

	"github.com/sonic/src/graph/gbackends"
)

func TestGraph(t *testing.T) {
	var db gbackends.BoltBackEnd
	err := db.Open("test.db")
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	g := NewGraph(&db)
	_ = g.Put([]byte("carlo"), []byte("ciao"))
	fmt.Println(db.Len())
	os.Remove("test.db")
}

func TestGraphEdgeMethods(t *testing.T) {
	db := gbackends.NewMapBackEnd()
	//err := db.Open("test.db")
	//if err != nil {
	//		t.Error(err)
	//	}
	//	defer db.Close()

	g := NewGraph(db)

	b, err := NewAny([]byte("ciao carlo"))
	if err != nil {
		t.Error(err)
	}

	err = g.AddEdge("tar", b)
	if err != nil {
		t.Error(err)
	}

	e, err := g.GetEdge("tar")
	if err != nil {
		t.Error(err)
	}
	v, err := g.Get([]byte("tar"))
	if err != nil {
		t.Error(err)
	}

	fmt.Printf("Getting value from backend :\n")
	fmt.Printf("Data, %v\n", e.Data())
	fmt.Printf("Byte, %v\n", v)
	var s string
	err = e.Deserialize(&s)
	if err != nil {
		t.Error(err)
	}

	//fmt.Printf("String is: %s\n", s)

	os.Remove("test.db")
}
func TestGraphAddDelPut(t *testing.T) {
	var db gbackends.BoltBackEnd
	err := db.Open("test.db")
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	g := NewGraph(&db)
	_ = g.Put([]byte("carlo"), []byte("ciao"))
	val, err := g.Get([]byte("carlo"))
	if err != nil {
		t.Errorf("Error in Get %v\n", err)
	}
	t.Logf("Value retrieve successfully: %s\n", val)
	g.Del([]byte("carlo"))
	if db.Len() != 0 {
		t.Error("Failed to delete the key/value")
	}
	t.Log("Key/Value deleted!")
	err = PrettyPrint(db)
	if err != nil {
		t.Logf("Failed to print because: \n %s", err)
	}
	os.Remove("test.db")
}

func TestGraphMemory(t *testing.T) {
	db := gbackends.NewMapBackEnd()
	g := NewGraph(db)
	_ = g.Put([]byte("carlo"), []byte("locci"))
	val, err := g.Get([]byte("carlo"))
	if err != nil {
		t.Errorf("Error in Get %v\n", err)
	}
	t.Logf("Value is: %v\n", string(val))
	db.Del([]byte("carlo"))
}
