package studydiskv

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
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
	return d.WriteStream(key, bytes.NewReader(val), false)
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

	return d.writeStreamWithLock(pathKey, r, sync)
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
	mode := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	f, err := os.OpenFile(d.completeFilename(pathKey), mode, d.FilePerm)
	if err != nil {
		return nil, fmt.Errorf("open file: %s", err)
	}
	return f, nil
}

func (d *Diskv) writeStreamWithLock(pathKey *PathKey, r io.Reader, sync bool) error {
	if err := d.ensurePathWithLock(pathKey); err != nil {
		return fmt.Errorf("ensure path: %s", &err)
	}

	f, err := d.createKeyFileWithLock(pathKey)
	if err != nil {
		return fmt.Errorf("create key file: %s", err)
	}

	wc := io.WriteCloser(&nopWriteCloser{f})
	if d.Compression != nil {
		wc, err = d.Compression.Writer(f)
		if err != nil {
			f.Close()
			os.Remove(f.Name())
			return fmt.Errorf("compression writer: %s", err)
		}
	}

	if _, err := io.Copy(wc, r); err != nil {
		f.Close()
		os.Remove(f.Name())
		return fmt.Errorf("i/o copy: %s", err)
	}

	if err := wc.Close(); err != nil {
		f.Close()
		os.Remove(f.Name())
		return fmt.Errorf("compression close: %s", err)
	}

	if sync {
		if err := f.Sync(); err != nil {
			f.Close()
			os.Remove(f.Name())
			return fmt.Errorf("file sync: %s", err)
		}
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("file close: %s", err)
	}

	fullPath := d.completeFilename(pathKey)
	if f.Name() != fullPath {
		if err := os.Rename(f.Name(), fullPath); err != nil {
			os.Remove(f.Name())
			return fmt.Errorf("rename: %s", err)
		}
	}
	if d.Index != nil {
		d.Index.Insert(pathKey.originalKey)
	}

	d.bustCacheWithLock(pathKey.originalKey)
	return nil
}

func (d *Diskv) Import(srcFilename, dstKey string, move bool) (err error) {
	if dstKey == "" {
		return errEmpty
	}

	if fi, err := os.Stat(srcFilename); err != nil {
		return err
	} else if fi.IsDir() {
		return errImportDirectory
	}

	dstPathKey := d.transform(dstKey)

	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.ensurePathWithLock(dstPathKey); err != nil {
		return fmt.Errorf("ensure path: %s", err)
	}

	if move {
		if err := syscall.Rename(srcFilename, d.completeFilename(dstPathKey)); err == nil {
			d.bustCacheWithLock(dstPathKey.originalKey)
			return nil
		} else if err != syscall.EXDEV {
			return err
		}
	}

	f, err := os.Open(srcFilename)
	if err != nil {
		return err
	}
	defer f.Close()
	err = d.writeStreamWithLock(dstPathKey, f, false)
	if err == nil && move {
		err = os.Remove(srcFilename)
	}
	return err
}

func (d *Diskv) Read(key string) ([]byte, error) {
	rc, err := d.ReadStream(key, false)
	if err != nil {
		return []byte{}, err
	}
	defer rc.Close()
	return ioutil.ReadAll(rc)
}

func (d *Diskv) ReadString(key string) string {
	value, _ := d.Read(key)
	return string(value)
}

func (d *Diskv) ReadStream(key string, direct bool) (io.ReadCloser, error) {
	pathKey := d.transform(key)
	d.mu.Lock()
	defer d.mu.Unlock()

	if val, ok := d.cache[key]; ok {
		if !direct {
			buf := bytes.NewReader(val)
			if d.Compression != nil {
				return d.Compression.Reader(buf)
			}
			return ioutil.NopCloser(buf), nil
		}

		go func() {
			d.mu.Lock()
			defer d.mu.Unlock()
			d.uncacheWithLock(key, uint64(len(val)))
		}()
	}

	return d.readWithRLock(pathKey)
}

func (d *Diskv) readWithRLock(pathKey *PathKey) (io.ReadCloser, error) {
	filename := d.completeFilename(pathKey)

	fi, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}
	if fi.IsDir() {
		return nil, os.ErrNotExist
	}

	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	var r io.Reader
	if d.CacheSizeMax > 0 {
		r = newSiphon(f, d, pathKey.originalKey)
	} else {
		r = &closingReader{f}
	}

	var rc = io.ReadCloser(ioutil.NopCloser(r))
	if d.Compression != nil {
		rc, err = d.Compression.Reader(r)
		if err != nil {
			return nil, err
		}
	}
	return rc, nil
}

type closingReader struct {
	rc io.ReadCloser
}

func (cr closingReader) Read(p []byte) (int, error) {
	n, err := cr.rc.Read(p)

	if err == io.EOF {
		if closeErr := cr.rc.Close(); closeErr != nil {
			return n, closeErr
		}
	}
	return n, err
}

func (d *Diskv) ensurePathWithLock(pathKey *PathKey) error {
	return os.MkdirAll(d.pathFor(pathKey), d.PathPerm)
}

type siphon struct {
	f   *os.File
	d   *Diskv
	key string
	buf *bytes.Buffer
}

func newSiphon(f *os.File, d *Diskv, key string) io.Reader {
	return &siphon{
		f:   f,
		d:   d,
		key: key,
		buf: &bytes.Buffer{},
	}
}

func (s *siphon) Read(p []byte) (int, error) {
	n, err := s.f.Read(p)

	if err == nil {
		return s.buf.Write(p[0:n])
	}

	if err == io.EOF {
		s.d.cacheWithoutLock(s.key, s.buf.Bytes())
		if closerErr := s.f.Close(); closerErr != nil {
			return n, closerErr
		}
		return n, err
	}
	return n, err
}

func (d *Diskv) Erase(key string) error {
	pathKey := d.transform(key)
	d.mu.Lock()
	defer d.mu.Unlock()

	d.bustCacheWithLock(key)

	if d.Index != nil {
		d.Index.Delete(key)
	}

	filename := d.completeFilename(pathKey)
	if s, err := os.Stat(filename); err == nil {
		if s.IsDir() {
			return errBadKey
		}
		if err = os.RemoveAll(filename); err != nil {
			return err
		}
	} else {
		return err
	}

	d.pruneDirsWithLock(key)
	return nil
}

func (d *Diskv) EraseAll() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.cache = make(map[string][]byte)
	d.cacheSize = 0
	if d.TempDir != "" {
		os.RemoveAll(d.TempDir)
	}
	return os.RemoveAll(d.BasePath)
}

func (d *Diskv) Has(key string) bool {
	pathKey := d.transform(key)
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.cache[key]; ok {
		return true
	}

	filename := d.completeFilename(pathKey)
	s, err := os.Stat(filename)
	if err != nil {
		return false
	}
	if s.IsDir() {
		return false
	}

	return true
}

func (d *Diskv) Keys(cancel <-chan struct{}) <-chan string {
	return d.KeysPrefix("", cancel)
}

func (d *Diskv) KeysPrefix(prefix string, cancel <-chan struct{}) <-chan string {
	var prepath string
	if prefix == "" {
		prepath = d.BasePath
	} else {
		prefixKey := d.transform(prefix)
		prepath = d.pathFor(prefixKey)
	}
	c := make(chan string)
	go func() {
		filepath.Walk(prepath, d.walker(c, prefix, cancel))
		close(c)
	}()
	return c
}

func (d *Diskv) walker(c chan<- string, prefix string, cancel <-chan struct{}) filepath.WalkFunc {
	return func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, _ := filepath.Rel(d.BasePath, path)
		dir, file := filepath.Split(relPath)
		pathSplit := strings.Split(dir, string(filepath.Separator))
		pathSplit = pathSplit[:len(pathSplit)-1]

		pathKey := &PathKey{
			Path:     pathSplit,
			FileName: file,
		}

		key := d.InverseTransform(pathKey)

		if info.IsDir() || !strings.HasPrefix(key, prefix) {
			return nil
		}

		select {
		case c <- key:
		case <-cancel:
			return errCanceled
		}

		return nil
	}
}

func (d *Diskv) pathFor(pathKey *PathKey) string {
	return filepath.Join(d.BasePath, filepath.Join(pathKey.Path...))
}

func (d *Diskv) completeFilename(pathKey *PathKey) string {
	return filepath.Join(d.pathFor(pathKey), pathKey.FileName)
}

func (d *Diskv) cacheWithLock(key string, val []byte) error {
	d.bustCacheWithLock(key)

	valueSize := uint64(len(val))
	if err := d.ensureCacheSpaceWithLock(valueSize); err != nil {
		return fmt.Errorf("%s; not cacheing", err)
	}

	if (d.cacheSize + valueSize) > d.CacheSizeMax {
		panic(fmt.Sprintf("failed to make room for value (%d/%d)", valueSize, d.CacheSizeMax))
	}

	d.cache[key] = val
	d.cacheSize += valueSize
	return nil
}

func (d *Diskv) cacheWithoutLock(key string, val []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.cacheWithLock(key, val)
}

func (d *Diskv) bustCacheWithLock(key string) {
	if val, ok := d.cache[key]; ok {
		d.uncacheWithLock(key, uint64(len(val)))
	}
}

func (d *Diskv) uncacheWithLock(key string, sz uint64) {
	d.cacheSize -= sz
	delete(d.cache, key)
}

func (d *Diskv) pruneDirsWithLock(key string) error {
	pathList := d.transform(key).Path
	for i := range pathList {
		dir := filepath.Join(d.BasePath, filepath.Join(pathList[:len(pathList)-i]...))

		switch fi, err := os.Stat(dir); true {
		case err != nil:
			return err
		case !fi.IsDir():
			panic(fmt.Sprintf("corrupt dirstate at %s", dir))
		}

		nlinks, err := filepath.Glob(filepath.Join(dir, "*"))
		if err != nil {
			return err
		} else if len(nlinks) > 0 {
			return nil
		}
		if err = os.Remove(dir); err != nil {
			return err
		}
	}
	return nil
}

func (d *Diskv) ensureCacheSpaceWithLock(valueSize uint64) error {
	if valueSize > d.CacheSizeMax {
		return fmt.Errorf("value size (%d size) too large for cache (%d bytes)", valueSize, d.CacheSizeMax)
	}

	safe := func() bool {
		return (d.cacheSize + valueSize) <= d.CacheSizeMax
	}

	for key, val := range d.cache {
		if safe() {
			break
		}
		d.uncacheWithLock(key, uint64(len(val)))
	}

	if !safe() {
		panic(fmt.Sprintf("%d bytes still won't fit in the cache! (max %d bytes)", valueSize, d.CacheSizeMax))
	}

	return nil
}

type nopWriteCloser struct {
	io.Writer
}

func (wc *nopWriteCloser) Write(p []byte) (int, error) { return wc.Writer.Write(p) }
func (wc *nopWriteCloser) Close() error                { return nil }
