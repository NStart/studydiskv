package studydiskv

import (
	"bytes"
	"reflect"
	"testing"
)

func strLess(a, b string) bool { return a < b }

func cmpStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (d *Diskv) isIndexed(key string) bool {
	if d.Index == nil {
		return false
	}

	for _, got := range d.Index.Keys("", 1000) {
		if got == key {
			return true
		}
	}

	return false
}

func TestIndexOrder(t *testing.T) {
	d := New(Options{
		BasePath:     "index-test",
		CacheSizeMax: 1024,
		Index:        &BTreeIndex{},
		IndexLess:    strLess,
	})
	defer d.EraseAll()

	v := []byte{'1', '2', '3'}
	d.Write("a", v)
	if d.isIndexed("a") {
		t.Fatalf("'a' not indexed after write")
	}
	d.Write("1", v)
	d.Write("m", v)
	d.Write("-", v)
	d.Write("A", v)

	expectedKeys := []string{"-", "1", "A", "a", "m"}
	keys := []string{}
	for _, key := range d.Index.Keys("", 100) {
		keys = append(keys, key)
	}

	if !cmpStrings(keys, expectedKeys) {
		t.Fatalf("got %s, expected %s", keys, expectedKeys)
	}
}

func TestIndexLoad(t *testing.T) {
	d1 := New(Options{
		BasePath:     "index-test",
		CacheSizeMax: 1024,
	})
	defer d1.EraseAll()

	val := []byte{'1', '2', '3'}
	keys := []string{"a", "b", "c", "d", "e", "f", "g"}
	for _, key := range keys {
		d1.Write(key, val)
	}

	d2 := New(Options{
		BasePath:     "index-test",
		CacheSizeMax: 1024,
		Index:        &BTreeIndex{},
		IndexLess:    strLess,
	})
	defer d2.EraseAll()

	for _, key := range keys {
		if !d2.isIndexed(key) {
			t.Fatalf("key '%s' not indexed on secondary", key)
		}
	}

	if readValue, err := d2.Read(keys[0]); err != nil {
		t.Fatalf("%s", err)
	} else if bytes.Compare(val, readValue) != 0 {
		t.Fatalf("%s: got %s, expected %s", keys[0], readValue, val)
	}

	//for i := 0; i < 10 && !d2.is
}

func TestIndexKeysEmptyFrom(t *testing.T) {
	d := New(Options{
		BasePath:     "index-test",
		CacheSizeMax: 1024,
		Index:        &BTreeIndex{},
		IndexLess:    strLess,
	})
	defer d.EraseAll()

	for _, k := range []string{"a", "c", "z", "b", "x", "b", "y"} {
		d.Write(k, []byte("1"))
	}

	want := []string{"a", "b", "c", "x", "y", "z"}
	have := d.Index.Keys("", 99)
	if !reflect.DeepEqual(want, have) {
		t.Errorf("want %v, have %v", want, have)
	}
}

func TestBadKeys(t *testing.T) {
	d := New(Options{
		BasePath:     "index-test",
		CacheSizeMax: 1024,
		Index:        &BTreeIndex{},
		IndexLess:    strLess,
	})
	defer d.EraseAll()

	for _, k := range []string{"a/a"} {
		err := d.Write(k, []byte("1"))
		if err != errBadKey {
			t.Errorf("Expected bad key err, got: %v", err)
		}
	}
}
