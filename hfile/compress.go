package hfile

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
)

// Decompressor decompresses block data.
type Decompressor interface {
	Decompress(src []byte) ([]byte, error)
	String() string
}

type noneDecompressor struct{}

func (noneDecompressor) Decompress(src []byte) ([]byte, error) { return src, nil }
func (noneDecompressor) String() string                        { return "NONE" }

type gzipDecompressor struct{}

func (gzipDecompressor) Decompress(src []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(src))
	if err != nil {
		return nil, fmt.Errorf("gzip new reader: %w", err)
	}
	defer r.Close()
	return io.ReadAll(r)
}

func (gzipDecompressor) String() string { return "GZ" }

// Compression codec ordinals (matching HBase Algorithm enum).
const (
	CompressionGZ   = 1 // HBase GZ compression ordinal
	CompressionNone = 2 // HBase NONE compression ordinal
)

// DecompressorForCodec returns the decompressor for the given compression codec ordinal.
func DecompressorForCodec(codec uint32) (Decompressor, error) {
	switch codec {
	case CompressionGZ:
		return gzipDecompressor{}, nil
	case CompressionNone:
		return noneDecompressor{}, nil
	default:
		return nil, fmt.Errorf("hfile: unsupported compression codec %d", codec)
	}
}
