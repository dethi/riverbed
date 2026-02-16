package hfile

import (
	"encoding/binary"
	"fmt"
	"io"
)

// FileInfo key for max tags length.
const fileInfoMaxTagsLen = "hfile.MAX_TAGS_LEN"

// Reader reads an HFile (v3 only).
type Reader struct {
	r        io.ReaderAt
	fileSize int64
	decomp   Decompressor

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

// Scanner iterates over cells in an HFile in key order.
type Scanner struct {
	rd       *Reader
	blockIdx int // current index into dataBlocks
	cellIter *CellIterator
	cell     *Cell
	err      error
	started  bool

	// dataBlocks holds the flat list of data block entries.
	// For single-level indexes, this is the root index entries directly.
	// For multi-level indexes, this is built by traversing the index tree.
	dataBlocks []IndexEntry
}

// Next advances to the next cell. Returns false when done or on error.
func (s *Scanner) Next() bool {
	if s.err != nil {
		return false
	}

	// Lazily resolve the flat list of data block entries on first call.
	if s.dataBlocks == nil {
		entries, err := s.resolveDataBlocks()
		if err != nil {
			s.err = err
			return false
		}
		s.dataBlocks = entries
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

		// Move to the next data block.
		if s.started {
			s.blockIdx++
		}
		s.started = true

		if s.blockIdx >= len(s.dataBlocks) {
			return false // no more blocks
		}

		entry := s.dataBlocks[s.blockIdx]
		blk, err := ReadBlock(s.rd.r, entry.BlockOffset, s.rd.decomp)
		if err != nil {
			s.err = fmt.Errorf("hfile: read data block %d: %w", s.blockIdx, err)
			return false
		}

		s.cellIter = NewCellIterator(blk.Data, s.rd.includeTags)
	}
}

// resolveDataBlocks returns the flat list of data block entries by traversing
// the index tree. For single-level indexes, this is the root entries directly.
func (s *Scanner) resolveDataBlocks() ([]IndexEntry, error) {
	idx := s.rd.dataIndex
	if idx.NumDataIndexLevels <= 1 {
		return idx.Entries, nil
	}

	// Multi-level: root entries point to intermediate or leaf blocks.
	// Traverse the tree to collect all leaf-level data block entries.
	var dataBlocks []IndexEntry
	for i, entry := range idx.Entries {
		entries, err := s.collectLeafEntries(entry.BlockOffset, idx.NumDataIndexLevels-1)
		if err != nil {
			return nil, fmt.Errorf("hfile: resolve data blocks from root entry %d: %w", i, err)
		}
		dataBlocks = append(dataBlocks, entries...)
	}
	return dataBlocks, nil
}

// collectLeafEntries recursively descends the index tree from a non-root block.
// remainingLevels indicates how many more levels to descend (1 = this is a leaf).
func (s *Scanner) collectLeafEntries(offset int64, remainingLevels uint32) ([]IndexEntry, error) {
	entries, err := ReadNonRootIndex(s.rd.r, offset, s.rd.decomp)
	if err != nil {
		return nil, err
	}

	if remainingLevels <= 1 {
		// These entries point directly to data blocks.
		return entries, nil
	}

	// Intermediate level: recurse into children.
	var result []IndexEntry
	for _, entry := range entries {
		children, err := s.collectLeafEntries(entry.BlockOffset, remainingLevels-1)
		if err != nil {
			return nil, err
		}
		result = append(result, children...)
	}
	return result, nil
}

// Cell returns the current cell.
func (s *Scanner) Cell() *Cell { return s.cell }

// Err returns any error encountered during scanning.
func (s *Scanner) Err() error { return s.err }
