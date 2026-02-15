package hfile

import "fmt"

// Decompressor decompresses block data.
type Decompressor interface {
	Decompress(src []byte) ([]byte, error)
	String() string
}

type noneDecompressor struct{}

func (noneDecompressor) Decompress(src []byte) ([]byte, error) { return src, nil }
func (noneDecompressor) String() string                        { return "NONE" }

// Compression codec ordinals (matching HBase Algorithm enum).
const (
	CompressionNone = 2 // HBase NONE compression ordinal
)

// DecompressorForCodec returns the decompressor for the given compression codec ordinal.
func DecompressorForCodec(codec uint32) (Decompressor, error) {
	switch codec {
	case CompressionNone:
		return noneDecompressor{}, nil
	default:
		return nil, fmt.Errorf("hfile: unsupported compression codec %d", codec)
	}
}
