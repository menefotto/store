// The graph package provide a generelized data structure ideal for manupulating
// graph in a simple fashion, by providing a backend to it, let's you choose if
// use want to have persistency of a ram based approach.
// To obtain generalization the edges are a simple interface the implements the
// Data() []bytes method, down here there is an example Any, in practise an edge
// must serialize itself and take care of it thanks to the go standard library
// this is quite an easy job.

package graph

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/sonic/lib/graph/gbackends"
)

// defines the Edge interface
type Edge interface {
	Serialize(v interface{}) ([]byte, error)
	Deserialize(v interface{}) error
	Data() []byte
}

//implements an Edge
type Any struct {
	Buffer []byte
}

func NewAny(v interface{}) (*Any, error) {
	a := &Any{Buffer: make([]byte, 0)}
	_, err := a.Serialize(v)
	if err != nil {
		return a, err
	}
	return a, nil
}

func (a *Any) Serialize(v interface{}) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	a.Buffer = append(a.Buffer, b...)

	return b, nil
}

func (a *Any) Deserialize(v interface{}) error {
	err := json.Unmarshal(a.Buffer, v)
	if err != nil {
		return err
	}

	return nil
}

func (a *Any) Data() []byte { return a.Buffer }

//graph implementation

type Graph struct {
	gbackends.DB
}

var ErrNotSupported error = fmt.Errorf("back end not supported")

func NewGraph(g gbackends.DB) *Graph {
	return &Graph{g}
}

func (g *Graph) AddEdge(key string, e Edge) error {

	err := g.Put([]byte(key), e.Data())
	if err != nil {
		return err
	}

	return nil
}

func (g *Graph) DelEdge(key string) {
	g.Del([]byte(key))

}

func (g *Graph) GetEdge(key string) (*Any, error) {
	val, err := g.Get([]byte(key))
	if err != nil {
		return nil, err
	}

	return &Any{Buffer: val}, nil
}

func (g *Graph) BackEnd() gbackends.DB {
	return g
}

//helper functions for debugging memory backend

func MarshallToDisk(g interface{}) error {
	switch g.(type) {
	case *gbackends.MapBackEnd:
		b, err := json.Marshal(g)
		if err != nil {
			return err
		}

		f, err := os.Create("graph.dump")
		defer f.Close()
		if err != nil {
			return err
		}

		n, err := f.Write(b)
		if n != len(b) || err != nil {
			return err
		}
	default:
		return ErrNotSupported
	}
	return nil
}

func UnmarshalFromDisk(g interface{}) error {
	switch g.(type) {
	case *gbackends.MapBackEnd:
		f, err := os.Open("graph.dump")
		defer f.Close()
		if err != nil {
			return err
		}

		stat, err := f.Stat()
		if err != nil {
			return err
		}

		data := make([]byte, stat.Size())
		count, err := f.Read(data)
		if count != int(stat.Size()) || err != nil {
			return fmt.Errorf("missmatched size\n")
		}

		err = json.Unmarshal(data, g)
		if err != nil {
			return err
		}
	default:
		return ErrNotSupported
	}

	return nil
}

func PrettyPrint(g interface{}) error {
	switch t := g.(type) {
	case *gbackends.MapBackEnd:
		for key, value := range t.Db {
			fmt.Println("-----------------------------------------")
			fmt.Println("Key is: ", key)
			for _, val := range value {
				fmt.Printf("\t\t Value is: %v\n", val)
			}
			fmt.Println("-----------------------------------------")
		}
		return nil
	default:
		return ErrNotSupported

	}
}
