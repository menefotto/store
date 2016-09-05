// package sonicdb implementes the backends for
// the graph data type. It is defined by an interface
// containing three methods Get Put and Delete
// anything that satisfy this interface can be used
// and has to be used during the graph datatype initialization
// The MapBackEnd type exposed is an in memory backed db implementation
// and it's used for testing any database has to implement this methods
// to satisfy the interface and can be used as a backend
// One note on the Del method in case of failure of that
// is key not found is should not return anything or do
// anything much like the MapBackEnd type in the go language itself.
// BoltBackEnd provides an transactional
// backend implemented using boltdb provides 5 methods
// Get, Put, Del, Open and Close, three of these are required
// by the db interface

package gbackends

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/boltdb/bolt"
)

type DB interface {
	Get(value []byte) ([]byte, error)
	Put(key []byte, value []byte) error
	Del(key []byte)
	Query(key []byte, t string) (map[string][]byte, error)
	Close() error
}

var ErrNotFound error = fmt.Errorf("key not found")

type BoltBackEnd struct {
	Db         *bolt.DB
	bucketname string
}

func NewBoltBackEnd(dbname string) (*BoltBackEnd, error) {
	return getBoltBackEnd(dbname)
}

func GetBoltBackEnd(dbname string) (*BoltBackEnd, error) {
	return getBoltBackEnd(dbname)
}

func getBoltBackEnd(dbname string) (*BoltBackEnd, error) {
	var db BoltBackEnd
	err := db.Open(dbname)
	if err != nil {
		return nil, err
	}
	return &db, nil
}

func (b *BoltBackEnd) Open(name string) error {
	var err error
	b.Db, err = bolt.Open(name, 0600, nil)
	if err != nil {
		defer b.Db.Close()
		return err
	}

	b.bucketname = "all"

	_ = b.Db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(b.bucketname))
		if err != nil {
			defer b.Db.Close()
			return err
		}
		return nil
	})

	return nil
}

func (b *BoltBackEnd) Close() error {
	return b.Db.Close()
}

func (b *BoltBackEnd) Get(value []byte) (result []byte, err error) {
	// err assigned directly to err named return value
	err = b.Db.View(func(tx *bolt.Tx) error {
		buck := tx.Bucket([]byte(b.bucketname))
		// not checking for exisistance since it always exist
		result = buck.Get(value)
		// value assigned directly to named return value
		if result == nil {
			return ErrNotFound
		}
		return nil
	})
	return
}

func (b *BoltBackEnd) Put(key []byte, value []byte) error {
	err := b.Db.Update(func(tx *bolt.Tx) error {
		buck := tx.Bucket([]byte(b.bucketname))
		// not checking since it alway exist
		err := buck.Put(key, value)
		if err != nil {
			return err
		}
		return nil
	})
	return err

}

func (b *BoltBackEnd) Del(key []byte) {
	_ = b.Db.Update(func(tx *bolt.Tx) error {
		buck := tx.Bucket([]byte(b.bucketname))
		// not checking for error since it always exist
		buck.Delete(key)
		// not checking for error since it always exist
		// and in case it doesn't we why raise an error
		// follow the MapBackEnd delete api
		return nil
	})
	return
}

func (b *BoltBackEnd) Len() (value int) {
	// err is not taken into consideration since it always nil
	_ = b.Db.View(func(tx *bolt.Tx) error {
		buck := tx.Bucket([]byte(b.bucketname))
		// not checking for exisistance since it always exist
		stat := buck.Stats()
		// value assigned directly to named return value
		value = stat.KeyN
		return nil
	})
	return
}

func (b *BoltBackEnd) Query(name []byte, t string) (result map[string][]byte, err error) {
	err = b.Db.View(func(tx *bolt.Tx) error {
		buck := tx.Bucket([]byte(b.bucketname))
		// got the bucket now get the cursor
		c := buck.Cursor()
		// got the cursor now iterates over the k, values
		result = make(map[string][]byte, 0)
		// iterate over the elements using hasPrefix
		var search func(s, name []byte) bool

		switch {
		case t == "p":
			search = bytes.HasPrefix
			name = name[:len(name)-1]
		case t == "s":
			search = bytes.HasSuffix
			name = name[1:]
		}

		for k, v := c.Seek(name); search(k, name); k, v = c.Next() {
			result[string(k)] = v
		}

		return nil
	})

	return
}

type MapBackEnd struct {
	Db map[string]string
}

func NewMapBackEnd() *MapBackEnd {
	return &MapBackEnd{Db: make(map[string]string, 0)}
}

func (m *MapBackEnd) Get(key []byte) ([]byte, error) {
	value, ok := m.Db[string(key)]
	if ok {
		return []byte(value), nil
	}
	return nil, ErrNotFound
}

func (m *MapBackEnd) Put(key []byte, value []byte) error {
	m.Db[string(key)] = string(value)
	// always return nil it cannot fail if it fails MapBackEnd implementaion
	// is going to panic no point double check again
	return nil

}

func (m *MapBackEnd) Del(key []byte) {
	delete(m.Db, string(key))
}

func (m *MapBackEnd) Close() error {

	// does nothing here to satisfy the DB interface
	return nil
}

func (m *MapBackEnd) Query(key []byte, str string) (map[string][]byte, error) {
	return nil, errors.New("not implemented")
}
