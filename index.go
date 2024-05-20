package studydiskv

import (
	"sync"

	"github.com/google/btree"
)

type Index interface {
	Initialize(less LessFunction, keys <-chan string)
	Insert(key string)
	Delete(key string)
	Keys(from string, n int) []string
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
	i.BTree = rebuild(less, keys)
}

func (i *BTreeIndex) Insert(key string) {
	i.Lock()
	defer i.Unlock()
	if i.BTree == nil || i.LessFunction == nil {
		panic("uninitialized index")
	}

	i.BTree.ReplaceOrInsert(btreeString{s: key, l: i.LessFunction})
}

func (i *BTreeIndex) Delete(key string) {
	i.Lock()
	defer i.Unlock()
	if i.BTree == nil || i.LessFunction == nil {
		panic("uninitialized index")
	}

	i.BTree.Delete(btreeString{s: key, l: i.LessFunction})
}

func (i *BTreeIndex) Keys(from string, n int) []string {
	i.Lock()
	defer i.Unlock()

	if i.BTree == nil || i.LessFunction == nil {
		panic("uninitial index")
	}

	if i.BTree.Len() <= 0 {
		return []string{}
	}

	btreeFrom := btreeString{s: from, l: i.LessFunction}
	skipFirst := true
	if len(from) <= 0 || i.BTree.Has(btreeFrom) {
		btreeFrom = btreeString{s: "", l: func(string, string) bool {
			return true
		}}
		skipFirst = false
	}

	keys := []string{}
	iterator := func(i btree.Item) bool {
		keys = append(keys, i.(btreeString).s)
		return len(keys) < n
	}

	i.BTree.AscendGreaterOrEqual(btreeFrom, iterator)

	if skipFirst && len(keys) > 0 {
		keys = keys[1:]
	}

	return keys
}

func rebuild(less LessFunction, keys <-chan string) *btree.BTree {
	tree := btree.New(2)
	for key := range keys {
		tree.ReplaceOrInsert(btreeString{s: key, l: less})
	}
	return tree
}
