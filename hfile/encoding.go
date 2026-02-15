package hfile

import "fmt"

// DataBlockDecoder decodes data block payloads based on the encoding type.
type DataBlockDecoder interface {
	Decode(src []byte) ([]byte, error)
	String() string
}

type noneDecoder struct{}

func (noneDecoder) Decode(src []byte) ([]byte, error) { return src, nil }
func (noneDecoder) String() string                    { return "NONE" }

// Data block encoding IDs (matching HBase DataBlockEncoding enum ordinals).
const (
	EncodingNone = 0
)

// DataBlockDecoderFor returns the decoder for the given encoding ID.
func DataBlockDecoderFor(id int) (DataBlockDecoder, error) {
	switch id {
	case EncodingNone:
		return noneDecoder{}, nil
	default:
		return nil, fmt.Errorf("hfile: unsupported data block encoding %d", id)
	}
}
