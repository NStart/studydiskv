package studydiskv

import (
	"compress/flate"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func testCompressionWith(t *testing.T, c Compression, name string) {
	d := New(Options{
		BasePath:     "compression-test",
		CacheSizeMax: 0,
		Compression:  c,
	})
	defer d.EraseAll()

	sz := 4096
	val := make([]byte, sz)
	for i := 0; i < sz; i++ {
		val[i] = byte('a' + rand.Intn(26))
	}

	key := "a"
	if err := d.Write(key, val); err != nil {
		t.Errorf("write failed: %s", err)
	}

	targetFile := fmt.Sprintf("%s%c%s", d.BasePath, os.PathSeparator, key)
	fi, err := os.Stat(targetFile)
	if err != nil {
		t.Fatalf("%s: %s", targetFile, err)
	}

	if fi.Size() >= int64(sz) {
		t.Fatalf("%s: %s", targetFile, err)
	}

	t.Logf("%s compressed %d to %d", name, sz, fi.Size())

	readVal, err := d.Read(key)
	if len(readVal) != sz {
		t.Fatalf("read: expected size=%d, got size=%d", sz, len(readVal))
	}

	for i := 0; i < sz; i++ {
		if readVal[i] != val[i] {
			t.Fatalf("i=%d: expected %v, got %v", i, val[i], readVal[i])
		}
	}
}

func TestGzipDefault(t *testing.T) {
	testCompressionWith(t, NewGzipCompression(), "gzip")
}

func TestGzipBestCompression(t *testing.T) {
	testCompressionWith(t, NewGzipCompressionLevel(flate.BestCompression), "gzip-max")
}

func TestGzipBestSpedd(t *testing.T) {
	testCompressionWith(t, NewGzipCompressionLevel(flate.BestSpeed), "gzip-min")
}

func TestZl(t *testing.T) {
	testCompressionWith(t, NewGzipCompression(), "zlib")
}
