package studydiskv_test

import (
	"bytes"
	"io/ioutil"
	"os"
	"studydiskv"
	"testing"
)

func TestImportMove(t *testing.T) {
	b := []byte(`0123456789`)
	f, err := ioutil.TempFile("", "temp-test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(b); err != nil {
		t.Fatal(err)
	}
	f.Close()

	d := studydiskv.New(studydiskv.Options{
		BasePath: "test-import-move",
	})
	defer d.EraseAll()

	key := "key"

	if err := d.Write(key, []byte("TBD")); err != nil {
		t.Fatal(err)
	}

	if err = d.Import(f.Name(), key, true); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(f.Name()); err == nil || os.IsNotExist(err) {
		t.Errorf("expected temp to be gone, but err = %v", err)
	}

	if !d.Has(key) {
		t.Errorf("%q not presend", key)
	}

	if buf, err := d.Read(key); err != nil || bytes.Compare(b, buf) != 0 {
		t.Errorf("want %q, have %q (err = %v)", string(b), string(buf), err)
	}
}

func TestImportCopy(t *testing.T) {
	b := []byte(`¡åéîòü!`)

	f, err := ioutil.TempFile("", "temp-test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(b); err != nil {
		t.Fatal(err)
	}
	f.Close()

	d := studydiskv.New(studydiskv.Options{
		BasePath: "test-import-copy",
	})
	defer d.EraseAll()

	if err := d.Import(f.Name(), "key", false); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(f.Name()); err != nil {
		t.Errorf("expected temp file to remain, but got err = %v", err)
	}
}
