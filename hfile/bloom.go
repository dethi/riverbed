package hfile

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

// BloomFilter holds a compound bloom filter read from an HFile.
type BloomFilter struct {
	TotalByteSize int64
	HashCount     int32
	HashType      int32
	TotalKeyCount int64
	TotalMaxKeys  int64
	NumChunks     int32
	Comparator    string
	ChunkIndex    *BlockIndex

	r      io.ReaderAt
	decomp Decompressor
}

// ReadBloomFilter reads the bloom filter metadata from a GENERAL_BLOOM_META block.
func ReadBloomFilter(r io.ReaderAt, offset int64, decomp Decompressor) (*BloomFilter, error) {
	blk, err := ReadBlock(r, offset, decomp)
	if err != nil {
		return nil, fmt.Errorf("hfile: read bloom meta block: %w", err)
	}
	if blk.Header.Type != BlockGeneralBloomMeta {
		return nil, fmt.Errorf("hfile: expected GENERAL_BLOOM_META block, got %s", blk.Header.Type)
	}

	data := blk.Data
	if len(data) < 40 { // 4(version)+8+4+4+8+8+4
		return nil, fmt.Errorf("hfile: bloom meta block too small")
	}

	bf := &BloomFilter{
		r:      r,
		decomp: decomp,
	}

	off := 0
	version := int32(binary.BigEndian.Uint32(data[off : off+4]))
	off += 4
	if version != 3 {
		return nil, fmt.Errorf("hfile: unsupported bloom filter version %d", version)
	}
	bf.TotalByteSize = int64(binary.BigEndian.Uint64(data[off : off+8]))
	off += 8
	bf.HashCount = int32(binary.BigEndian.Uint32(data[off : off+4]))
	off += 4
	bf.HashType = int32(binary.BigEndian.Uint32(data[off : off+4]))
	off += 4
	bf.TotalKeyCount = int64(binary.BigEndian.Uint64(data[off : off+8]))
	off += 8
	bf.TotalMaxKeys = int64(binary.BigEndian.Uint64(data[off : off+8]))
	off += 8
	bf.NumChunks = int32(binary.BigEndian.Uint32(data[off : off+4]))
	off += 4

	// Read comparator class name (VInt-prefixed byte array).
	comparator, n, err := readByteArray(data, off)
	if err != nil {
		return nil, fmt.Errorf("hfile: read bloom comparator: %w", err)
	}
	bf.Comparator = string(comparator)
	off += n

	// Read the chunk block index (same format as root index).
	idx, _, err := parseRootIndex(data[off:], int(bf.NumChunks))
	if err != nil {
		return nil, fmt.Errorf("hfile: read bloom chunk index: %w", err)
	}
	bf.ChunkIndex = idx

	return bf, nil
}

// MayContain checks whether the given row key may be present in the bloom filter.
func (bf *BloomFilter) MayContain(key []byte) (bool, error) {
	if bf.NumChunks == 0 {
		return true, nil
	}

	// Find which chunk this key belongs to by searching the chunk index.
	chunkIdx := bf.findChunk(key)
	if chunkIdx < 0 {
		return false, nil
	}

	// Read the bloom chunk block.
	entry := bf.ChunkIndex.Entries[chunkIdx]
	blk, err := ReadBlock(bf.r, entry.BlockOffset, bf.decomp)
	if err != nil {
		return false, fmt.Errorf("hfile: read bloom chunk %d: %w", chunkIdx, err)
	}

	return bf.containsInChunk(key, blk.Data), nil
}

func (bf *BloomFilter) findChunk(key []byte) int {
	// Binary search the chunk index for the last entry whose key <= given key.
	entries := bf.ChunkIndex.Entries
	lo, hi := 0, len(entries)-1
	result := -1
	for lo <= hi {
		mid := lo + (hi-lo)/2
		cmp := compareBytes(key, entries[mid].Key)
		if cmp >= 0 {
			result = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return result
}

func (bf *BloomFilter) containsInChunk(key, bloomData []byte) bool {
	hash1 := murmurHash(key, 0)
	hash2 := murmurHash(key, hash1)

	bloomBitSize := len(bloomData) * 8
	if bloomBitSize == 0 {
		return false
	}

	compositeHash := hash1
	for range int(bf.HashCount) {
		hashLoc := int(math.Abs(float64(compositeHash % int32(bloomBitSize))))
		if !checkBit(bloomData, hashLoc) {
			return false
		}
		compositeHash += hash2
	}
	return true
}

func checkBit(data []byte, pos int) bool {
	bytePos := pos >> 3
	bitPos := pos & 0x7
	return data[bytePos]&(1<<uint(bitPos)) != 0
}

// murmurHash implements MurmurHash2 as used by HBase.
func murmurHash(key []byte, seed int32) int32 {
	const m int32 = 0x5bd1e995
	const r = 24

	length := len(key)
	h := seed ^ int32(length)

	nblocks := length >> 2
	for i := range nblocks {
		i4 := i << 2
		k := int32(key[i4+3])
		k = k << 8
		k = k | int32(key[i4+2]&0xff)
		k = k << 8
		k = k | int32(key[i4+1]&0xff)
		k = k << 8
		k = k | int32(key[i4]&0xff)

		k *= m
		k ^= int32(uint32(k) >> r)
		k *= m
		h *= m
		h ^= k
	}

	tail := nblocks << 2
	left := length - tail
	if left >= 3 {
		h ^= int32(int8(key[tail+2])) << 16
	}
	if left >= 2 {
		h ^= int32(int8(key[tail+1])) << 8
	}
	if left >= 1 {
		h ^= int32(int8(key[tail]))
		h *= m
	}

	h ^= int32(uint32(h) >> 13)
	h *= m
	h ^= int32(uint32(h) >> 15)

	return h
}

func compareBytes(a, b []byte) int {
	minLen := min(len(b), len(a))
	for i := range minLen {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}
