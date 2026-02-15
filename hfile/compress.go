package hfile

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
)

// Decompressor decompresses block data. The uncompressedSize parameter is the
// expected size of the decompressed output (from the block header), used to
// pre-allocate buffers.
type Decompressor interface {
	Decompress(src []byte, uncompressedSize int) ([]byte, error)
	String() string
}

type noneDecompressor struct{}

func (noneDecompressor) Decompress(src []byte, _ int) ([]byte, error) { return src, nil }
func (noneDecompressor) String() string                               { return "NONE" }

type gzipDecompressor struct{}

func (gzipDecompressor) Decompress(src []byte, uncompressedSize int) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(src))
	if err != nil {
		return nil, fmt.Errorf("gzip new reader: %w", err)
	}
	defer r.Close()
	buf := make([]byte, uncompressedSize)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("gzip read: %w", err)
	}
	return buf, nil
}

func (gzipDecompressor) String() string { return "GZ" }

type snappyDecompressor struct{}

func (snappyDecompressor) Decompress(src []byte, uncompressedSize int) ([]byte, error) {
	out := make([]byte, 0, uncompressedSize)
	return hadoopBlockDecompress(out, src, func(dst, chunk []byte) ([]byte, error) {
		// snappy.Decode writes from the start of its dst, so we give it
		// the tail of our output buffer and append the result.
		off := len(dst)
		tail := dst[off:cap(dst)]
		decoded, err := snappy.Decode(tail, chunk)
		if err != nil {
			return nil, err
		}
		// If snappy used our tail, just reslice. Otherwise append.
		if len(decoded) <= cap(tail) && &decoded[0] == &tail[0] {
			return dst[:off+len(decoded)], nil
		}
		return append(dst, decoded...), nil
	})
}

func (snappyDecompressor) String() string { return "SNAPPY" }

type zstdDecompressor struct{}

func (zstdDecompressor) Decompress(src []byte, uncompressedSize int) ([]byte, error) {
	r, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("zstd new reader: %w", err)
	}
	defer r.Close()
	out := make([]byte, 0, uncompressedSize)
	return hadoopBlockDecompress(out, src, func(dst, chunk []byte) ([]byte, error) {
		return r.DecodeAll(chunk, dst)
	})
}

func (zstdDecompressor) String() string { return "ZSTD" }

// hadoopBlockDecompress parses the Hadoop BlockCompressorStream framing format
// and decompresses each chunk using the provided raw decompressor function.
//
// The framing format is:
//
//	[decompressed block size - 4 bytes big-endian]
//	  [compressed chunk size - 4 bytes big-endian]
//	  [compressed chunk data]
//	  ... (repeat until decompressed bytes >= decompressed block size)
//	... (repeat blocks until input exhausted)
//
// The raw decompress function appends decompressed data to dst and returns it.
func hadoopBlockDecompress(out, src []byte, rawDecompress func(dst, chunk []byte) ([]byte, error)) ([]byte, error) {
	pos := 0

	for pos < len(src) {
		if pos+4 > len(src) {
			return nil, fmt.Errorf("hadoop block decompress: truncated block size at offset %d", pos)
		}
		decompressedBlockSize := int(binary.BigEndian.Uint32(src[pos : pos+4]))
		pos += 4

		decompressedInBlock := 0
		for decompressedInBlock < decompressedBlockSize {
			if pos+4 > len(src) {
				return nil, fmt.Errorf("hadoop block decompress: truncated chunk size at offset %d", pos)
			}
			compressedChunkSize := int(binary.BigEndian.Uint32(src[pos : pos+4]))
			pos += 4

			if pos+compressedChunkSize > len(src) {
				return nil, fmt.Errorf("hadoop block decompress: truncated chunk data at offset %d", pos)
			}
			chunk := src[pos : pos+compressedChunkSize]
			pos += compressedChunkSize

			prevLen := len(out)
			var err error
			out, err = rawDecompress(out, chunk)
			if err != nil {
				return nil, fmt.Errorf("hadoop block decompress: %w", err)
			}
			decompressedInBlock += len(out) - prevLen
		}
	}

	return out, nil
}

// Compression codec ordinals (matching HBase Algorithm enum).
const (
	CompressionGZ     = 1 // HBase GZ compression ordinal
	CompressionNone   = 2 // HBase NONE compression ordinal
	CompressionSnappy = 3 // HBase SNAPPY compression ordinal
	CompressionZSTD   = 6 // HBase ZSTD compression ordinal
)

// DecompressorForCodec returns the decompressor for the given compression codec ordinal.
func DecompressorForCodec(codec uint32) (Decompressor, error) {
	switch codec {
	case CompressionGZ:
		return gzipDecompressor{}, nil
	case CompressionNone:
		return noneDecompressor{}, nil
	case CompressionSnappy:
		return snappyDecompressor{}, nil
	case CompressionZSTD:
		return zstdDecompressor{}, nil
	default:
		return nil, fmt.Errorf("hfile: unsupported compression codec %d", codec)
	}
}
