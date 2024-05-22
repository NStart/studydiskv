package studydiskv

import (
	"bytes"
	"io/ioutil"
	"testing"
	"time"
)

func TestIssue2A(t *testing.T) {
	d := New(Options{
		BasePath:     "test-issue-2a",
		CacheSizeMax: 1024,
	})
	defer d.EraseAll()

	input := "abcdefghijklmnopqrstuvwxy"
	key, writeBuf, sync := "a", bytes.NewBufferString(input), false
	if err := d.WriteStream(key, writeBuf, sync); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		began := time.Now()
		rc, err := d.ReadStream(key, false)
		if err != nil {
			t.Fatal(err)
		}
		buf, err := ioutil.ReadAll(rc)
		if err != nil {
			t.Fatal(err)
		}
		//if !cmpB
		rc.Close()
		t.Logf("read #%d in %s", i+1, time.Since(began))
	}
}
