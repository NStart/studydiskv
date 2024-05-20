package studydiskv

import (
	"sync"

	"github.com/google/btree"
)

type Index interface {
	Initialize(less LessFunction, keys <-chan string)
}

type LessFunction func(string, string) bool

type btreeString struct {
	s string
	l LessFunction
}

func (s btreeString) Less(i btree.Item) bool {
	return s.l(s.s, i.(btreeString).s)
}

type BTreeIndex struct {
	sync.RWMutex
	LessFunction
	*btree.BTree
}

func (i *BTreeIndex) InitialLize(less LessFunction, keys <-chan string) {
	i.Lock()
	defer i.Unlock()
	i.LessFunction = less
	//i.BTree =
}

func (i *BTreeIndex) Insert(key string) {
	i.Lock()
	defer i.Unlock()
	if i.BTree == nil || i.LessFunction == nil {
		panic("uninitialized index")
	}

}
