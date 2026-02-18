package hfile

import (
	"encoding/binary"
	"fmt"
)

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
	EncodingNone     = 0
	EncodingFastDiff = 4
)

// DataBlockDecoderFor returns the decoder for the given encoding ID.
// includeTags indicates whether the encoded stream contains tag data.
func DataBlockDecoderFor(id int, includeTags bool) (DataBlockDecoder, error) {
	switch id {
	case EncodingNone:
		return noneDecoder{}, nil
	case EncodingFastDiff:
		return fastDiffDecoder{includeTags: includeTags}, nil
	default:
		return nil, fmt.Errorf("hfile: unsupported data block encoding %d", id)
	}
}

// readCompressedInt reads a 7-bit variable-length encoded integer
// (HBase ByteBufferUtils format). Each byte contributes 7 value bits (LSB first),
// with bit 7 as a continuation flag.
func readCompressedInt(buf []byte, offset int) (int, int, error) {
	result := 0
	shift := 0
	for i := offset; i < len(buf); i++ {
		b := buf[i]
		if shift >= 31 {
			return 0, 0, fmt.Errorf("hfile: compressed int overflow at offset %d", offset)
		}
		result |= int(b&0x7f) << shift
		shift += 7
		if b&0x80 == 0 {
			return result, i - offset + 1, nil
		}
	}
	return 0, 0, fmt.Errorf("hfile: compressed int extends past end of buffer at offset %d", offset)
}

// FAST_DIFF flag bits.
const (
	maskTimestampLength = 0x07 // bits 0-2: common timestamp prefix length
	flagSameKeyLength   = 0x08 // bit 3
	flagSameValueLength = 0x10 // bit 4
	flagSameType        = 0x20 // bit 5
	flagSameValue       = 0x40 // bit 6
)

// fastDiffDecoder decodes FAST_DIFF encoded data blocks into NONE format.
type fastDiffDecoder struct {
	includeTags bool
}

func (fastDiffDecoder) String() string { return "FAST_DIFF" }

func (d fastDiffDecoder) Decode(src []byte) ([]byte, error) {
	if len(src) < 4 {
		return nil, fmt.Errorf("hfile: FAST_DIFF block too short for decompressed size")
	}
	decompressedSize := int(binary.BigEndian.Uint32(src[:4]))
	out := make([]byte, 0, decompressedSize)
	pos := 4

	var (
		keyLen            int
		valLen            int
		keyBuf            []byte // reconstructed key buffer
		prevValue         []byte
		rowLenWithSize    int // rowLength + 2 (row length field size)
		famLenWithSize    int // familyLength + 1 (family length field size)
		prevTimestampType [9]byte
		isFirst           = true
	)

	for pos < len(src) {
		flag := src[pos]
		pos++

		// Save previous timestamp+type before key length changes.
		if !isFirst && flag&flagSameKeyLength == 0 {
			copy(prevTimestampType[:], keyBuf[keyLen-9:keyLen])
		}

		// Read key length.
		if flag&flagSameKeyLength == 0 {
			v, n, err := readCompressedInt(src, pos)
			if err != nil {
				return nil, fmt.Errorf("hfile: FAST_DIFF read key length: %w", err)
			}
			keyLen = v
			pos += n
		}

		// Read value length.
		if flag&flagSameValueLength == 0 {
			v, n, err := readCompressedInt(src, pos)
			if err != nil {
				return nil, fmt.Errorf("hfile: FAST_DIFF read value length: %w", err)
			}
			valLen = v
			pos += n
		}

		// Read common prefix length.
		commonPrefix, n, err := readCompressedInt(src, pos)
		if err != nil {
			return nil, fmt.Errorf("hfile: FAST_DIFF read common prefix: %w", err)
		}
		pos += n

		// Ensure key buffer is large enough.
		if len(keyBuf) < keyLen {
			newBuf := make([]byte, keyLen)
			copy(newBuf, keyBuf)
			keyBuf = newBuf
		}

		// Key bytes excluding timestamp(8) + type(1).
		keyDataLen := keyLen - 9

		if commonPrefix > keyDataLen {
			return nil, fmt.Errorf("hfile: FAST_DIFF common prefix %d exceeds key data length %d", commonPrefix, keyDataLen)
		}

		if isFirst {
			// First cell: read the entire key (excluding timestamp+type).
			if pos+keyDataLen > len(src) {
				return nil, fmt.Errorf("hfile: FAST_DIFF first cell key data extends past buffer")
			}
			copy(keyBuf[commonPrefix:keyDataLen], src[pos:pos+(keyDataLen-commonPrefix)])
			pos += keyDataLen - commonPrefix

			rowLenWithSize = int(binary.BigEndian.Uint16(keyBuf[:2])) + 2
			if rowLenWithSize >= keyDataLen {
				return nil, fmt.Errorf("hfile: FAST_DIFF row length %d exceeds key data length %d", rowLenWithSize, keyDataLen)
			}
			famLenWithSize = int(keyBuf[rowLenWithSize]) + 1
			if rowLenWithSize+famLenWithSize > keyDataLen {
				return nil, fmt.Errorf("hfile: FAST_DIFF row+family length %d exceeds key data length %d", rowLenWithSize+famLenWithSize, keyDataLen)
			}
		} else if commonPrefix < 2 {
			// Row length bytes changed.
			oldRowLenWithSize := rowLenWithSize

			// Read the non-common row length bytes.
			need := 2 - commonPrefix
			if pos+need > len(src) {
				return nil, fmt.Errorf("hfile: FAST_DIFF row length extends past buffer")
			}
			copy(keyBuf[commonPrefix:2], src[pos:pos+need])
			pos += need
			rowLenWithSize = int(binary.BigEndian.Uint16(keyBuf[:2])) + 2
			if rowLenWithSize >= keyDataLen {
				return nil, fmt.Errorf("hfile: FAST_DIFF row length %d exceeds key data length %d", rowLenWithSize, keyDataLen)
			}

			// Ensure key buffer is large enough after row length change.
			if len(keyBuf) < keyLen {
				newBuf := make([]byte, keyLen)
				copy(newBuf, keyBuf)
				keyBuf = newBuf
			}

			// Move family to its new position.
			copy(keyBuf[rowLenWithSize:rowLenWithSize+famLenWithSize],
				keyBuf[oldRowLenWithSize:oldRowLenWithSize+famLenWithSize])

			// Read rest of row.
			rowRest := rowLenWithSize - 2
			if pos+rowRest > len(src) {
				return nil, fmt.Errorf("hfile: FAST_DIFF row data extends past buffer")
			}
			copy(keyBuf[2:rowLenWithSize], src[pos:pos+rowRest])
			pos += rowRest

			// Read qualifier.
			qualLen := keyDataLen - rowLenWithSize - famLenWithSize
			if qualLen < 0 {
				return nil, fmt.Errorf("hfile: FAST_DIFF negative qualifier length")
			}
			if pos+qualLen > len(src) {
				return nil, fmt.Errorf("hfile: FAST_DIFF qualifier extends past buffer")
			}
			copy(keyBuf[rowLenWithSize+famLenWithSize:], src[pos:pos+qualLen])
			pos += qualLen

		} else if commonPrefix < rowLenWithSize {
			// Part of row differs but row length is the same.
			rowRest := rowLenWithSize - commonPrefix
			if pos+rowRest > len(src) {
				return nil, fmt.Errorf("hfile: FAST_DIFF row suffix extends past buffer")
			}
			copy(keyBuf[commonPrefix:rowLenWithSize], src[pos:pos+rowRest])
			pos += rowRest

			// Read qualifier.
			qualLen := keyDataLen - rowLenWithSize - famLenWithSize
			if qualLen < 0 {
				return nil, fmt.Errorf("hfile: FAST_DIFF negative qualifier length")
			}
			if pos+qualLen > len(src) {
				return nil, fmt.Errorf("hfile: FAST_DIFF qualifier extends past buffer")
			}
			copy(keyBuf[rowLenWithSize+famLenWithSize:], src[pos:pos+qualLen])
			pos += qualLen

		} else {
			// Common prefix covers row + family; read qualifier suffix.
			suffixLen := keyDataLen - commonPrefix
			if suffixLen < 0 {
				return nil, fmt.Errorf("hfile: FAST_DIFF negative suffix length")
			}
			if pos+suffixLen > len(src) {
				return nil, fmt.Errorf("hfile: FAST_DIFF qualifier suffix extends past buffer")
			}
			copy(keyBuf[commonPrefix:commonPrefix+suffixLen], src[pos:pos+suffixLen])
			pos += suffixLen
		}

		// Timestamp: common prefix bytes from previous, rest from stream.
		tsPos := keyLen - 9
		commonTsPrefix := int(flag & maskTimestampLength)
		if !isFirst && flag&flagSameKeyLength == 0 {
			// Key length changed; restore common timestamp bytes from saved buffer.
			copy(keyBuf[tsPos:tsPos+commonTsPrefix], prevTimestampType[:commonTsPrefix])
		}
		// If key length is same, timestamp common prefix bytes are already in place.
		tsSuffix := 8 - commonTsPrefix
		if pos+tsSuffix > len(src) {
			return nil, fmt.Errorf("hfile: FAST_DIFF timestamp extends past buffer")
		}
		copy(keyBuf[tsPos+commonTsPrefix:tsPos+8], src[pos:pos+tsSuffix])
		pos += tsSuffix

		// Type byte.
		typePos := keyLen - 1
		if isFirst || flag&flagSameType == 0 {
			if pos >= len(src) {
				return nil, fmt.Errorf("hfile: FAST_DIFF type byte extends past buffer")
			}
			keyBuf[typePos] = src[pos]
			pos++
		} else if flag&flagSameKeyLength == 0 {
			// Key length changed but type is same; restore from saved.
			keyBuf[typePos] = prevTimestampType[8]
		}
		// If both flagSameType and flagSameKeyLength are set, the type is
		// already in place at the correct offset (key length didn't change).

		// Value.
		var value []byte
		if isFirst || flag&flagSameValue == 0 {
			if pos+valLen > len(src) {
				return nil, fmt.Errorf("hfile: FAST_DIFF value extends past buffer")
			}
			value = src[pos : pos+valLen]
			pos += valLen
		} else {
			value = prevValue
		}

		// Write NONE format: keyLen(4) + valLen(4) + key + value.
		out = binary.BigEndian.AppendUint32(out, uint32(keyLen))
		out = binary.BigEndian.AppendUint32(out, uint32(valLen))
		out = append(out, keyBuf[:keyLen]...)
		out = append(out, value...)

		// Tags: read compressed int length from encoded stream, write as 2-byte big-endian.
		if d.includeTags {
			tagsLen, tn, err := readCompressedInt(src, pos)
			if err != nil {
				return nil, fmt.Errorf("hfile: FAST_DIFF read tags length: %w", err)
			}
			pos += tn
			out = binary.BigEndian.AppendUint16(out, uint16(tagsLen))
			if tagsLen > 0 {
				if pos+tagsLen > len(src) {
					return nil, fmt.Errorf("hfile: FAST_DIFF tags data extends past buffer")
				}
				out = append(out, src[pos:pos+tagsLen]...)
				pos += tagsLen
			}
		}

		// memstoreTS: copy the raw VInt bytes from source to output.
		if pos < len(src) {
			_, vn, err := readVInt(src, pos)
			if err != nil {
				return nil, fmt.Errorf("hfile: FAST_DIFF read memstoreTS: %w", err)
			}
			out = append(out, src[pos:pos+vn]...)
			pos += vn
		}

		// Save value for potential reuse.
		if isFirst || flag&flagSameValue == 0 {
			prevValue = make([]byte, valLen)
			copy(prevValue, value)
		}

		isFirst = false
	}

	return out, nil
}
