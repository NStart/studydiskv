package studydiskv

import (
	"compress/zlib"
	"io"
)

type Compression interface {
	Writer(dst io.Writer) (io.WriteCloser, error)
	Reader(src io.Reader) (io.ReadCloser, error)
}

func NewGzipCompression(level int) Compression {
	return NewZipCompressionLevelDict(level, nil)
}

func NewZipCompressionLevelDict(level int, dict []byte) Compression {
	return &genericCompression{
		func(w io.Writer) (io.WriteCloser, error) {
			return zlib.NewWriterLevelDict(w, level, dict)
		},
		func(r io.Reader) (io.ReadCloser, error) {
			return zlib.NewReaderDict(r, dict)
		},
	}
}

type genericCompression struct {
	wf func(w io.Writer) (io.WriteCloser, error)
	rf func(r io.Reader) (io.ReadCloser, error)
}

func (g *genericCompression) Writer(dst io.Writer) (io.WriteCloser, error) {
	return g.wf(dst)
}

func (g *genericCompression) Reader(src io.Reader) (io.ReadCloser, error) {
	return g.rf(src)
}
