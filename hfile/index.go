package hfile

import (
	"encoding/binary"
	"fmt"
	"io"
)

// IndexEntry represents a single entry in a block index.
type IndexEntry struct {
	BlockOffset int64
	DataSize    int32
	Key         []byte
}

// BlockIndex holds the parsed root block index.
type BlockIndex struct {
	Entries            []IndexEntry
	NumDataIndexLevels uint32
	// Multi-level index metadata (only set when NumDataIndexLevels > 1).
	MidLeafBlockOffset     int64
	MidLeafBlockOnDiskSize int32
	MidKeyEntry            int32
}

// ReadRootIndex reads the root data index from the load-on-open section.
// The block at loadOnOpenOffset is a ROOT_INDEX block.
func ReadRootIndex(r io.ReaderAt, offset int64, numEntries int, numLevels uint32, decomp Decompressor) (*BlockIndex, error) {
	blk, err := ReadBlock(r, offset, decomp)
	if err != nil {
		return nil, fmt.Errorf("hfile: read root index block: %w", err)
	}

	idx, consumed, err := parseRootIndex(blk.Data, numEntries)
	if err != nil {
		return nil, err
	}
	idx.NumDataIndexLevels = numLevels

	// For multi-level indexes, read mid-key metadata after the index entries.
	if numLevels > 1 {
		remaining := blk.Data[consumed:]
		if len(remaining) >= 16 { // 8 + 4 + 4
			idx.MidLeafBlockOffset = int64(binary.BigEndian.Uint64(remaining[0:8]))
			idx.MidLeafBlockOnDiskSize = int32(binary.BigEndian.Uint32(remaining[8:12]))
			idx.MidKeyEntry = int32(binary.BigEndian.Uint32(remaining[12:16]))
		}
	}

	return idx, nil
}

func parseRootIndex(data []byte, numEntries int) (*BlockIndex, int, error) {
	entries := make([]IndexEntry, 0, numEntries)
	offset := 0

	for i := range numEntries {
		if offset+12 > len(data) {
			return nil, 0, fmt.Errorf("hfile: root index entry %d: not enough data", i)
		}
		blockOffset := int64(binary.BigEndian.Uint64(data[offset : offset+8]))
		dataSize := int32(binary.BigEndian.Uint32(data[offset+8 : offset+12]))
		offset += 12

		key, n, err := readByteArray(data, offset)
		if err != nil {
			return nil, 0, fmt.Errorf("hfile: root index entry %d key: %w", i, err)
		}
		offset += n

		entries = append(entries, IndexEntry{
			BlockOffset: blockOffset,
			DataSize:    dataSize,
			Key:         key,
		})
	}

	return &BlockIndex{Entries: entries}, offset, nil
}

// ReadMetaIndex reads the meta block index.
func ReadMetaIndex(r io.ReaderAt, offset int64, numEntries int, decomp Decompressor) (*BlockIndex, error) {
	if numEntries == 0 {
		return &BlockIndex{}, nil
	}

	blk, err := ReadBlock(r, offset, decomp)
	if err != nil {
		return nil, fmt.Errorf("hfile: read meta index block: %w", err)
	}

	idx, _, err := parseRootIndex(blk.Data, numEntries)
	if err != nil {
		return nil, fmt.Errorf("hfile: parse meta index: %w", err)
	}
	return idx, nil
}

// ReadLeafIndex reads a leaf index block and returns its entries.
func ReadLeafIndex(r io.ReaderAt, offset int64, decomp Decompressor) ([]IndexEntry, error) {
	blk, err := ReadBlock(r, offset, decomp)
	if err != nil {
		return nil, fmt.Errorf("hfile: read leaf index block: %w", err)
	}
	if blk.Header.Type != BlockLeafIndex {
		return nil, fmt.Errorf("hfile: expected LEAF_INDEX block, got %s", blk.Header.Type)
	}

	// Leaf index format: numEntries (int32) followed by entries.
	if len(blk.Data) < 4 {
		return nil, fmt.Errorf("hfile: leaf index block too small")
	}
	numEntries := int(binary.BigEndian.Uint32(blk.Data[0:4]))
	data := blk.Data[4:]

	entries := make([]IndexEntry, 0, numEntries)
	off := 0
	for i := range numEntries {
		if off+12 > len(data) {
			return nil, fmt.Errorf("hfile: leaf index entry %d: not enough data", i)
		}
		blockOffset := int64(binary.BigEndian.Uint64(data[off : off+8]))
		dataSize := int32(binary.BigEndian.Uint32(data[off+8 : off+12]))
		off += 12

		key, n, err := readByteArray(data, off)
		if err != nil {
			return nil, fmt.Errorf("hfile: leaf index entry %d key: %w", i, err)
		}
		off += n

		entries = append(entries, IndexEntry{
			BlockOffset: blockOffset,
			DataSize:    dataSize,
			Key:         key,
		})
	}
	return entries, nil
}
