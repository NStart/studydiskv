package studydiskv

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"sync"
)

const (
	defaultBasePath             = "diskv"
	defaultFilePerm os.FileMode = 0666
	defaultPathPerm os.FileMode = 0777
)

type PathKey struct {
	Path        []string
	FileName    string
	originalKey string
}

var (
	defaultAdvancedTransform = func(s string) *PathKey { return &PathKey{Path: []string{}, FileName: s} }
	defaultInverseTransform  = func(pathKey *PathKey) string { return pathKey.FileName }
	errCanceled              = errors.New("canceled")
	errEmpty                 = errors.New("empty key")
	errBadKey                = errors.New("bad key")
	errImportDirectory       = errors.New("can't import a directory")
)

type TransformFunction func(s string) []string

type AdvancedTransformFunction func(s string) *PathKey

type InverseTransformFunction func(pathkey *PathKey) string

type Options struct {
	BasePath          string
	Transform         TransformFunction
	AdvancedTransform AdvancedTransformFunction
	InverseTransform  InverseTransformFunction
	CacheSizeMax      uint64
	PathPerm          os.FileMode
	FilePerm          os.FileMode
	TempDir           string
	Index             Index
	IndexLess         LessFunction
	Compression       Compression
}

type Diskv struct {
	Options
	mu        sync.RWMutex
	cache     map[string][]byte
	cacheSize uint64
}

func New(o Options) *Diskv {
	if o.BasePath == "" {
		o.BasePath = defaultBasePath
	}

	if o.AdvancedTransform == nil {
		if o.Transform == nil {
			o.AdvancedTransform = defaultAdvancedTransform
		} else {
			o.AdvancedTransform = convertToAdvancedTransform(o.Transform)
		}
		if o.InverseTransform == nil {
			o.InverseTransform = defaultInverseTransform
		}
	} else {
		if o.InverseTransform == nil {
			panic("You must provide an InverseTransform function in advanced mode")
		}
	}

	if o.PathPerm == 0 {
		o.PathPerm = defaultPathPerm
	}
	if o.FilePerm == 0 {
		o.FilePerm = defaultFilePerm
	}

	d := &Diskv{
		Options:   o,
		cache:     map[string][]byte{},
		cacheSize: 0,
	}

	if d.Index != nil && d.IndexLess != nil {
		d.Index.Initialize(d.IndexLess, d.Keys(nil))
	}

	return d
}

func convertToAdvancedTransform(oldFunc func(s string) []string) AdvancedTransformFunction {
	return func(s string) *PathKey {
		return &PathKey{Path: oldFunc(s), FileName: s}
	}
}

func (d *Diskv) Write(key string, val []byte) error {
	//return
}

func (d *Diskv) WriteString(key string, val string) error {
	return d.Write(key, []byte(val))
}

func (d *Diskv) transform(key string) (pathKey *PathKey) {
	pathKey = d.AdvancedTransform(key)
	pathKey.originalKey = key
	return pathKey
}

func (d *Diskv) WriteStream(key string, r io.Reader, sync bool) error {
	if len(key) <= 0 {
		return errEmpty
	}

	pathKey := d.transform(key)

	for _, pathPart := range pathKey.Path {
		if strings.ContainsRune(pathPart, os.PathSeparator) {
			return errBadKey
		}
	}

	if strings.ContainsRune(pathKey.FileName, os.PathListSeparator) {
		return errBadKey
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	//return d
}

func (d *Diskv) createKeyFileWithLock(pathKey *PathKey) (*os.File, error) {
	if d.TempDir != "" {
		if err := os.MkdirAll(d.TempDir, d.PathPerm); err != nil {
			return nil, fmt.Errorf("temp mkdir: %s", err)
		}
		f, err := ioutil.TempFile(d.TempDir, "")
		if err != nil {
			return nil, fmt.Errorf("temp file: %s", err)
		}

		if err := os.Chmod(f.Name(), d.FilePerm); err != nil {
			f.Close()
			os.Remove(f.Name())
			return nil, fmt.Errorf("chomod: %s", err)
		}
		return f, nil
	}
	//mode := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	//f, err = os.OpenFile(d.comple)
}

func (d *Diskv) writeStreamWithLock(pathKey *PathKey, r io.Reader, sync bool) error {
	//if err := d
}
