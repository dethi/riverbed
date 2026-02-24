package hfile

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Block type magic strings (8 bytes each).
const magicLen = 8

var (
	magicData              = [magicLen]byte{'D', 'A', 'T', 'A', 'B', 'L', 'K', '*'}
	magicEncodedData       = [magicLen]byte{'D', 'A', 'T', 'A', 'B', 'L', 'K', 'E'}
	magicLeafIndex         = [magicLen]byte{'I', 'D', 'X', 'L', 'E', 'A', 'F', '2'}
	magicBloomChunk        = [magicLen]byte{'B', 'L', 'M', 'F', 'B', 'L', 'K', '2'}
	magicMeta              = [magicLen]byte{'M', 'E', 'T', 'A', 'B', 'L', 'K', 'c'}
	magicIntermediateIndex = [magicLen]byte{'I', 'D', 'X', 'I', 'N', 'T', 'E', '2'}
	magicRootIndex         = [magicLen]byte{'I', 'D', 'X', 'R', 'O', 'O', 'T', '2'}
	magicFileInfo          = [magicLen]byte{'F', 'I', 'L', 'E', 'I', 'N', 'F', '2'}
	magicGeneralBloomMeta  = [magicLen]byte{'B', 'L', 'M', 'F', 'M', 'E', 'T', '2'}
	magicDeleteBloomMeta   = [magicLen]byte{'D', 'F', 'B', 'L', 'M', 'E', 'T', '2'}
	magicTrailer           = [magicLen]byte{'T', 'R', 'A', 'B', 'L', 'K', '"', '$'}
	magicIndexV1           = [magicLen]byte{'I', 'D', 'X', 'B', 'L', 'K', ')', '+'}
)

// BlockType represents the type of an HFile block.
type BlockType int

const (
	BlockData BlockType = iota
	BlockEncodedData
	BlockLeafIndex
	BlockBloomChunk
	BlockMeta
	BlockIntermediateIndex
	BlockRootIndex
	BlockFileInfo
	BlockGeneralBloomMeta
	BlockDeleteBloomMeta
	BlockTrailer
	BlockIndexV1
)

var blockTypeNames = [...]string{
	"DATA", "ENCODED_DATA", "LEAF_INDEX", "BLOOM_CHUNK", "META",
	"INTERMEDIATE_INDEX", "ROOT_INDEX", "FILE_INFO",
	"GENERAL_BLOOM_META", "DELETE_FAMILY_BLOOM_META", "TRAILER", "INDEX_V1",
}

func (bt BlockType) String() string {
	if int(bt) < len(blockTypeNames) {
		return blockTypeNames[bt]
	}
	return fmt.Sprintf("UNKNOWN(%d)", bt)
}

var magicToBlockType = map[[magicLen]byte]BlockType{
	magicData:              BlockData,
	magicEncodedData:       BlockEncodedData,
	magicLeafIndex:         BlockLeafIndex,
	magicBloomChunk:        BlockBloomChunk,
	magicMeta:              BlockMeta,
	magicIntermediateIndex: BlockIntermediateIndex,
	magicRootIndex:         BlockRootIndex,
	magicFileInfo:          BlockFileInfo,
	magicGeneralBloomMeta:  BlockGeneralBloomMeta,
	magicDeleteBloomMeta:   BlockDeleteBloomMeta,
	magicTrailer:           BlockTrailer,
	magicIndexV1:           BlockIndexV1,
}

// HFile v3 block header size: 8 (magic) + 4 + 4 + 8 + 1 + 4 + 4 = 33
const blockHeaderSize = 33

// BlockHeader contains the parsed header of an HFile block.
type BlockHeader struct {
	Type                     BlockType
	OnDiskSizeWithoutHeader  int32
	UncompressedSize         int32
	PrevBlockOffset          int64
	ChecksumType             byte
	BytesPerChecksum         int32
	OnDiskDataSizeWithHeader int32
}

// Block is a parsed HFile block with its header and decompressed data payload.
type Block struct {
	Header BlockHeader
	Data   []byte // decompressed payload (without header)
	Offset int64  // file offset where this block was read from
}

func parseBlockType(magic [magicLen]byte) (BlockType, error) {
	bt, ok := magicToBlockType[magic]
	if !ok {
		return 0, fmt.Errorf("hfile: unknown block magic %q", magic)
	}
	return bt, nil
}

// ReadBlockHeader reads and parses a block header at the given offset.
func ReadBlockHeader(r io.ReaderAt, offset int64) (BlockHeader, error) {
	var buf [blockHeaderSize]byte
	if _, err := r.ReadAt(buf[:], offset); err != nil {
		return BlockHeader{}, fmt.Errorf("hfile: read block header at %d: %w", offset, err)
	}
	return parseBlockHeader(buf[:])
}

func parseBlockHeader(buf []byte) (BlockHeader, error) {
	var magic [magicLen]byte
	copy(magic[:], buf[:magicLen])
	bt, err := parseBlockType(magic)
	if err != nil {
		return BlockHeader{}, err
	}
	return BlockHeader{
		Type:                     bt,
		OnDiskSizeWithoutHeader:  int32(binary.BigEndian.Uint32(buf[8:12])),
		UncompressedSize:         int32(binary.BigEndian.Uint32(buf[12:16])),
		PrevBlockOffset:          int64(binary.BigEndian.Uint64(buf[16:24])),
		ChecksumType:             buf[24],
		BytesPerChecksum:         int32(binary.BigEndian.Uint32(buf[25:29])),
		OnDiskDataSizeWithHeader: int32(binary.BigEndian.Uint32(buf[29:33])),
	}, nil
}

// ReadBlock reads a complete block (header + data) at the given offset,
// verifies checksums, and decompresses the payload.
func ReadBlock(r io.ReaderAt, offset int64, decomp Decompressor) (*Block, error) {
	// Read the header first to learn the on-disk body size.
	var headerBuf [blockHeaderSize]byte
	if _, err := r.ReadAt(headerBuf[:], offset); err != nil {
		return nil, fmt.Errorf("hfile: read block header at %d: %w", offset, err)
	}
	hdr, err := parseBlockHeader(headerBuf[:])
	if err != nil {
		return nil, err
	}

	if hdr.OnDiskSizeWithoutHeader < 0 {
		return nil, fmt.Errorf("hfile: invalid on-disk size %d at offset %d", hdr.OnDiskSizeWithoutHeader, offset)
	}

	// Read the on-disk data after the header.
	onDisk := make([]byte, hdr.OnDiskSizeWithoutHeader)
	if _, err := r.ReadAt(onDisk, offset+blockHeaderSize); err != nil {
		return nil, fmt.Errorf("hfile: read block data at %d: %w", offset, err)
	}

	// The actual data size (excluding checksums) is onDiskDataSizeWithHeader - headerSize.
	dataSize := int(hdr.OnDiskDataSizeWithHeader) - blockHeaderSize
	if dataSize < 0 || dataSize > len(onDisk) {
		return nil, fmt.Errorf("hfile: invalid block data size %d at offset %d", dataSize, offset)
	}

	// Verify checksums.
	if hdr.ChecksumType != checksumNull {
		checksumData := onDisk[dataSize:]
		if err := verifyChecksums(hdr.ChecksumType, int(hdr.BytesPerChecksum), headerBuf[:], onDisk[:dataSize], checksumData); err != nil {
			return nil, fmt.Errorf("hfile: block at %d: %w", offset, err)
		}
	}

	// Validate uncompressed size before allocating.
	if hdr.UncompressedSize < 0 {
		return nil, fmt.Errorf("hfile: invalid uncompressed size %d at offset %d", hdr.UncompressedSize, offset)
	}

	// Decompress the data portion.
	payload, err := decomp.Decompress(onDisk[:dataSize], int(hdr.UncompressedSize))
	if err != nil {
		return nil, fmt.Errorf("hfile: decompress block at %d: %w", offset, err)
	}

	return &Block{
		Header: hdr,
		Data:   payload,
		Offset: offset,
	}, nil
}
