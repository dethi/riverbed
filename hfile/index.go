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
	// Each entry requires at least 12 bytes (offset + dataSize) plus 1 byte for the key
	// length varint. Cap the capacity to avoid huge allocations from corrupted metadata.
	cap := numEntries
	if maxEntries := len(data) / 13; cap > maxEntries {
		cap = maxEntries
	}
	entries := make([]IndexEntry, 0, cap)
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

// ReadNonRootIndex reads a non-root index block (LEAF_INDEX or INTERMEDIATE_INDEX)
// and returns its entries. Non-root blocks use a different format from root blocks:
//
//	[numEntries (4 bytes)]
//	[secondary index: (numEntries + 1) * 4 bytes — relative offsets to each entry]
//	[entries: each is offset(8) | onDiskSize(4) | key(variable, NO vint prefix)]
func ReadNonRootIndex(r io.ReaderAt, offset int64, decomp Decompressor) ([]IndexEntry, error) {
	blk, err := ReadBlock(r, offset, decomp)
	if err != nil {
		return nil, fmt.Errorf("hfile: read non-root index block: %w", err)
	}
	if blk.Header.Type != BlockLeafIndex && blk.Header.Type != BlockIntermediateIndex {
		return nil, fmt.Errorf("hfile: expected LEAF_INDEX or INTERMEDIATE_INDEX block, got %s", blk.Header.Type)
	}
	return parseNonRootIndex(blk.Data)
}

func parseNonRootIndex(data []byte) ([]IndexEntry, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("hfile: non-root index block too small")
	}
	numEntries := int(binary.BigEndian.Uint32(data[0:4]))
	if numEntries == 0 {
		return nil, nil
	}

	// Secondary index: (numEntries + 1) * 4 bytes of relative offsets.
	secIdxSize := (numEntries + 1) * 4
	secIdxStart := 4
	if len(data) < secIdxStart+secIdxSize {
		return nil, fmt.Errorf("hfile: non-root index block too small for secondary index")
	}

	// Read secondary index offsets (relative to start of entries section).
	secIdx := make([]int, numEntries+1)
	for i := range secIdx {
		secIdx[i] = int(binary.BigEndian.Uint32(data[secIdxStart+i*4 : secIdxStart+i*4+4]))
	}

	entriesStart := secIdxStart + secIdxSize
	entries := make([]IndexEntry, numEntries)
	for i := range numEntries {
		entryOff := entriesStart + secIdx[i]
		entryEnd := entriesStart + secIdx[i+1]
		if entryOff+12 > len(data) || entryEnd > len(data) {
			return nil, fmt.Errorf("hfile: non-root index entry %d: not enough data", i)
		}
		blockOffset := int64(binary.BigEndian.Uint64(data[entryOff : entryOff+8]))
		dataSize := int32(binary.BigEndian.Uint32(data[entryOff+8 : entryOff+12]))
		key := make([]byte, entryEnd-entryOff-12)
		copy(key, data[entryOff+12:entryEnd])

		entries[i] = IndexEntry{
			BlockOffset: blockOffset,
			DataSize:    dataSize,
			Key:         key,
		}
	}
	return entries, nil
}
