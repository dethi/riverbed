package hfile

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

// FileInfo key for max tags length.
const fileInfoMaxTagsLen = "hfile.MAX_TAGS_LEN"

// dataBlockEncodingID maps a DataBlockEncoding name to its numeric ID.
func dataBlockEncodingID(name string) (int, error) {
	switch name {
	case "NONE":
		return EncodingNone, nil
	case "FAST_DIFF":
		return EncodingFastDiff, nil
	default:
		return 0, fmt.Errorf("hfile: unsupported data block encoding %q", name)
	}
}

// Reader reads an HFile (v3 only).
type Reader struct {
	r          io.ReaderAt
	fileSize   int64
	decomp     Decompressor
	decoder    DataBlockDecoder
	encodingID int

	trailer     *Trailer
	dataIndex   *BlockIndex
	metaIndex   *BlockIndex
	fileInfo    map[string][]byte
	bloom       *BloomFilter // may be nil
	includeTags bool
}

// Open opens an HFile for reading. The caller provides an io.ReaderAt and the file size.
func Open(r io.ReaderAt, fileSize int64) (*Reader, error) {
	trailer, err := ReadTrailer(r, fileSize)
	if err != nil {
		return nil, err
	}

	decomp, err := DecompressorForCodec(trailer.CompressionCodec)
	if err != nil {
		return nil, err
	}

	rd := &Reader{
		r:        r,
		fileSize: fileSize,
		decomp:   decomp,
		trailer:  trailer,
	}

	// Read the load-on-open section, which starts at LoadOnOpenDataOffset.
	// It contains: root data index, then optionally meta index, file info, bloom meta.
	if err := rd.readLoadOnOpen(); err != nil {
		return nil, err
	}

	return rd, nil
}

func (rd *Reader) readLoadOnOpen() error {
	offset := int64(rd.trailer.LoadOnOpenDataOffset)

	// 1. Root data index block.
	dataIndex, err := ReadRootIndex(
		rd.r, offset,
		int(rd.trailer.DataIndexCount),
		rd.trailer.NumDataIndexLevels,
		rd.decomp,
	)
	if err != nil {
		return fmt.Errorf("hfile: read data index: %w", err)
	}
	rd.dataIndex = dataIndex

	// Calculate the next block offset from the root index block.
	hdr, err := ReadBlockHeader(rd.r, offset)
	if err != nil {
		return err
	}
	offset += blockHeaderSize + int64(hdr.OnDiskSizeWithoutHeader)

	// 2. Meta index block (always written by HBase, even when empty).
	if rd.trailer.MetaIndexCount > 0 {
		metaIndex, err := ReadMetaIndex(rd.r, offset, int(rd.trailer.MetaIndexCount), rd.decomp)
		if err != nil {
			return fmt.Errorf("hfile: read meta index: %w", err)
		}
		rd.metaIndex = metaIndex
	} else {
		rd.metaIndex = &BlockIndex{}
	}

	hdr, err = ReadBlockHeader(rd.r, offset)
	if err != nil {
		return err
	}
	offset += blockHeaderSize + int64(hdr.OnDiskSizeWithoutHeader)

	// 3. File info block.
	fileInfo, err := ReadFileInfo(rd.r, offset, rd.decomp)
	if err != nil {
		return fmt.Errorf("hfile: read file info: %w", err)
	}
	rd.fileInfo = fileInfo

	// Determine if cells include tags based on MAX_TAGS_LEN file info entry.
	if v, ok := fileInfo[fileInfoMaxTagsLen]; ok && len(v) == 4 {
		rd.includeTags = binary.BigEndian.Uint32(v) > 0
	}

	// Determine data block encoding and create decoder.
	encodingID := EncodingNone
	if v, ok := fileInfo[FileInfoDataBlockEncoding]; ok {
		id, err := dataBlockEncodingID(string(v))
		if err != nil {
			return err
		}
		encodingID = id
	}
	decoder, err := DataBlockDecoderFor(encodingID, rd.includeTags)
	if err != nil {
		return err
	}
	rd.decoder = decoder
	rd.encodingID = encodingID

	hdr, err = ReadBlockHeader(rd.r, offset)
	if err != nil {
		return err
	}
	offset += blockHeaderSize + int64(hdr.OnDiskSizeWithoutHeader)

	// 4. General bloom filter meta (optional).
	// Check if there's more data before the trailer.
	trailerOffset := rd.fileSize - trailerSize
	if offset < trailerOffset {
		// Try to read a bloom filter meta block.
		bloomHdr, err := ReadBlockHeader(rd.r, offset)
		if err == nil && bloomHdr.Type == BlockGeneralBloomMeta {
			bloom, err := ReadBloomFilter(rd.r, offset, rd.decomp)
			if err != nil {
				return fmt.Errorf("hfile: read bloom filter: %w", err)
			}
			rd.bloom = bloom
		}
	}

	return nil
}

// Trailer returns the parsed HFile trailer.
func (rd *Reader) Trailer() *Trailer { return rd.trailer }

// FileInfo returns the file info key-value map.
func (rd *Reader) FileInfo() map[string][]byte { return rd.fileInfo }

// NumEntries returns the total number of cells in the HFile.
func (rd *Reader) NumEntries() int64 { return int64(rd.trailer.EntryCount) }

// DataIndex returns the root data block index.
func (rd *Reader) DataIndex() *BlockIndex { return rd.dataIndex }

// MetaIndex returns the meta block index.
func (rd *Reader) MetaIndex() *BlockIndex { return rd.metaIndex }

// BloomFilter returns the bloom filter, or nil if none exists.
func (rd *Reader) BloomFilter() *BloomFilter { return rd.bloom }

// Scanner returns a new scanner for iterating over all cells in key order.
func (rd *Reader) Scanner() *Scanner {
	return &Scanner{rd: rd}
}

// indexLevel tracks the current position within one level of the index tree.
type indexLevel struct {
	entries []IndexEntry
	pos     int
}

// Scanner iterates over cells in an HFile in key order.
type Scanner struct {
	rd       *Reader
	cellIter *CellIterator
	cell     *Cell
	err      error
	started  bool

	// Index tree traversal state. The stack has one entry per index level,
	// with stack[0] being the root and stack[len-1] being the level whose
	// entries point directly to data blocks.
	stack []indexLevel
}

// Next advances to the next cell. Returns false when done or on error.
func (s *Scanner) Next() bool {
	if s.err != nil {
		return false
	}

	// Initialize the index stack on first call.
	if s.stack == nil {
		s.stack = []indexLevel{{entries: s.rd.dataIndex.Entries, pos: -1}}
	}

	for {
		// If we have a cell iterator, try to get the next cell.
		if s.cellIter != nil && s.cellIter.Next() {
			s.cell = s.cellIter.Cell()
			return true
		}
		if s.cellIter != nil {
			if err := s.cellIter.Err(); err != nil {
				s.err = err
				return false
			}
		}

		// Advance to the next data block entry.
		entry, ok := s.nextDataBlock()
		if !ok {
			return false
		}

		blk, err := ReadBlock(s.rd.r, entry.BlockOffset, s.rd.decomp)
		if err != nil {
			s.err = fmt.Errorf("hfile: read data block: %w", err)
			return false
		}

		data := blk.Data
		if blk.Header.Type == BlockEncodedData {
			// Encoded data blocks start with a 2-byte encoding ID prefix.
			if len(data) < 2 {
				s.err = fmt.Errorf("hfile: encoded data block too short")
				return false
			}
			blockEncodingID := int(binary.BigEndian.Uint16(data[:2]))
			if blockEncodingID != s.rd.encodingID {
				s.err = fmt.Errorf("hfile: data block encoding mismatch: block has %d, expected %d", blockEncodingID, s.rd.encodingID)
				return false
			}
			data, err = s.rd.decoder.Decode(data[2:])
			if err != nil {
				s.err = fmt.Errorf("hfile: decode data block: %w", err)
				return false
			}
		}

		s.cellIter = NewCellIterator(data, s.rd.includeTags)
	}
}

// nextDataBlock advances the index tree traversal and returns the next data
// block entry. For single-level indexes the stack has one level (root entries
// point directly to data blocks). For multi-level indexes, it descends through
// intermediate/leaf index blocks, loading them lazily.
func (s *Scanner) nextDataBlock() (IndexEntry, bool) {
	numLevels := max(int(s.rd.dataIndex.NumDataIndexLevels), 1)

	for {
		depth := len(s.stack) - 1

		// Advance the current (deepest) level.
		s.stack[depth].pos++

		// If we've exhausted this level, pop up.
		if s.stack[depth].pos >= len(s.stack[depth].entries) {
			if depth == 0 {
				return IndexEntry{}, false // done
			}
			s.stack = s.stack[:depth]
			continue
		}

		// If we're at the leaf level (depth == numLevels-1), return the entry.
		if depth >= numLevels-1 {
			return s.stack[depth].entries[s.stack[depth].pos], true
		}

		// Otherwise, descend: read the child index block and push it.
		entry := s.stack[depth].entries[s.stack[depth].pos]
		children, err := ReadNonRootIndex(s.rd.r, entry.BlockOffset, s.rd.decomp)
		if err != nil {
			s.err = fmt.Errorf("hfile: read index block at depth %d: %w", depth+1, err)
			return IndexEntry{}, false
		}
		s.stack = append(s.stack, indexLevel{entries: children, pos: -1})
	}
}

// Seek positions the scanner at the first cell whose key is >= the given key.
// The key uses the standard HBase cell key encoding: rowLen(2) + row + familyLen(1) +
// family + qualifier + timestamp(8) + type(1). It may be truncated at any field
// boundary to seek by prefix (e.g. just rowLen + row to seek to a row).
// Returns true if a cell was found, false if no cell >= key exists or on error.
// After Seek, Cell() returns the found cell and subsequent Next() calls continue
// from there.
func (s *Scanner) Seek(key []byte) bool {
	// Reset scanner state.
	s.err = nil
	s.cell = nil
	s.cellIter = nil
	s.started = true

	numLevels := max(int(s.rd.dataIndex.NumDataIndexLevels), 1)
	s.stack = make([]indexLevel, 0, numLevels)

	entries := s.rd.dataIndex.Entries
	for level := range numLevels {
		pos := searchIndex(entries, key)

		if level < numLevels-1 {
			// Intermediate level: descend into the child block.
			if pos < 0 {
				pos = 0
			}
			s.stack = append(s.stack, indexLevel{entries: entries, pos: pos})
			entry := entries[pos]
			children, err := ReadNonRootIndex(s.rd.r, entry.BlockOffset, s.rd.decomp)
			if err != nil {
				s.err = fmt.Errorf("hfile: seek: read index block at depth %d: %w", level+1, err)
				return false
			}
			entries = children
		} else {
			// Leaf level: set pos-1 so nextDataBlock() advances to the right block.
			if pos < 0 {
				pos = 0
			}
			s.stack = append(s.stack, indexLevel{entries: entries, pos: pos - 1})
		}
	}

	// Scan forward from the positioned block, skipping cells with key < seek key.
	for s.Next() {
		if compareHBaseKeys(cellKey(s.cell), key) >= 0 {
			return true
		}
	}
	return false
}

// searchIndex returns the index of the last entry whose Key <= key.
// Returns -1 if key is before all entries.
func searchIndex(entries []IndexEntry, key []byte) int {
	// sort.Search finds the first i where entries[i].Key > key.
	n := sort.Search(len(entries), func(i int) bool {
		return compareHBaseKeys(entries[i].Key, key) > 0
	})
	return n - 1
}

// compareHBaseKeys compares two HBase cell keys (or truncated seek-key prefixes)
// using HBase's CellComparatorImpl ordering:
//   - Rows are compared by byte content, ignoring the 2-byte length prefix.
//   - For equal rows, remaining fields are compared lexicographically.
//   - A truncated key (e.g. rowLen+row only) is considered less than any full
//     cell key with the same row.
func compareHBaseKeys(a, b []byte) int {
	aRow := hbaseKeyRow(a)
	bRow := hbaseKeyRow(b)
	if cmp := bytes.Compare(aRow, bRow); cmp != 0 {
		return cmp
	}
	// Same row content. Compare the fields that follow the row.
	aRest := a[2+len(aRow):]
	bRest := b[2+len(bRow):]
	if len(aRest) == 0 && len(bRest) == 0 {
		return 0
	}
	if len(aRest) == 0 {
		return -1 // a is a row-only prefix; b has more fields → a < b
	}
	if len(bRest) == 0 {
		return 1 // b is a row-only prefix; a has more fields → a > b
	}
	return bytes.Compare(aRest, bRest)
}

// hbaseKeyRow extracts the row bytes from a cell key or seek key.
// Key format: rowLen(2BE) + row + [optional remaining fields].
func hbaseKeyRow(key []byte) []byte {
	if len(key) < 2 {
		return nil
	}
	rowLen := int(binary.BigEndian.Uint16(key[:2]))
	end := 2 + rowLen
	if end > len(key) {
		end = len(key)
	}
	return key[2:end]
}

// cellKey builds the raw HBase cell key from a Cell's components.
func cellKey(c *Cell) []byte {
	keyLen := 2 + len(c.Row) + 1 + len(c.Family) + len(c.Qualifier) + 8 + 1
	key := make([]byte, keyLen)
	off := 0
	binary.BigEndian.PutUint16(key[off:], uint16(len(c.Row)))
	off += 2
	off += copy(key[off:], c.Row)
	key[off] = byte(len(c.Family))
	off++
	off += copy(key[off:], c.Family)
	off += copy(key[off:], c.Qualifier)
	binary.BigEndian.PutUint64(key[off:], c.Timestamp)
	off += 8
	key[off] = byte(c.Type)
	return key
}

// Cell returns the current cell.
func (s *Scanner) Cell() *Cell { return s.cell }

// Err returns any error encountered during scanning.
func (s *Scanner) Err() error { return s.err }
