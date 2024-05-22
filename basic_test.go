package studydiskv

import (
	"bytes"
	"testing"
	"time"
)

func cmpByte(a, b []byte) bool {
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

func (d *Diskv) isCache(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.cache[key]
	return ok
}

func TestWriteReadErase(t *testing.T) {
	d := New(Options{
		BasePath:     "test-data",
		CacheSizeMax: 1024,
	})
	defer d.EraseAll()
	k, v := "a", []byte{'b'}
	if err := d.Write(k, v); err != nil {
		t.Fatalf("write: %s", err)
	}
	if readVal, err := d.Read(k); err != nil {
		t.Fatalf("read: %s", err)
	} else if bytes.Compare(v, readVal) != 0 {
		t.Fatalf("read: expected %s, got %s", v, readVal)
	}
	if err := d.Erase(k); err != nil {
		t.Fatalf("erase: %s", err)
	}
}

func TestWRECache(t *testing.T) {
	d := New(Options{
		BasePath:     "test-data",
		CacheSizeMax: 1024,
	})

	defer d.EraseAll()
	k, v := "xxx", []byte{' ', ' ', ' '}
	if d.isCache(k) {
		t.Fatalf("key cached before Write and Read")
	}
	if err := d.Write(k, v); err != nil {
		t.Fatalf("write: %s", err)
	}
	if d.isCache(k) {
		t.Fatalf("key cached before Read")
	}
	if readVal, err := d.Read(k); err != nil {
		t.Fatalf("read: %s", err)
	} else if bytes.Compare(v, readVal) != 0 {
		t.Fatalf("read: expected %s, got %s", v, readVal)
	}
	for i := 0; i < 10 && !d.isCache(k); i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if !d.isCache(k) {
		t.Fatalf("key not cached after Read")
	}
	if err := d.Erase(k); err != nil {
		t.Fatalf("erase: %s", err)
	}
	if d.isCache(k) {
		t.Fatalf("key cached after Erase")
	}
}
