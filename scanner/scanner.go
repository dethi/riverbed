package scanner

import (
	"bytes"
	"encoding/binary"

	"github.com/dethi/riverbed/hfile"
)

// Options controls scanning behaviour.
type Options struct {
	// MaxVersions is the maximum number of versions to return per column.
	// 0 means unlimited (return all versions).
	MaxVersions int

	// StartKey is the row key to start scanning from (inclusive).
	// Empty means scan from the beginning of the region.
	StartKey []byte

	// EndKey is the row key to stop scanning at (exclusive).
	// Empty means scan to the end of the region.
	EndKey []byte
}

// RegionScanner merges N hfile.Scanner instances (for one region/family) into a
// single ordered stream, applying delete semantics and version limits.
//
// Pass the scanners slice newest-file-first so that tie-breaking assigns higher
// priority to newer files. Callers should have already filtered out split
// references.
type RegionScanner struct {
	heap    *kvHeap
	deletes *deleteTracker
	opts    Options

	// per-row state
	currentRow    []byte
	versionCounts map[string]int // qualifier → versions emitted so far

	cell *hfile.Cell
	err  error
}

// NewRegionScanner creates a RegionScanner over the given HFile scanners.
func NewRegionScanner(scanners []*hfile.Scanner, opts Options) (*RegionScanner, error) {
	var seekKey []byte
	if len(opts.StartKey) > 0 {
		seekKey = rowSeekKey(opts.StartKey)
	}
	h, err := newKVHeap(scanners, seekKey)
	if err != nil {
		return nil, err
	}
	return &RegionScanner{
		heap:          h,
		deletes:       newDeleteTracker(),
		opts:          opts,
		versionCounts: make(map[string]int),
	}, nil
}

// rowSeekKey encodes a row as a 2-byte big-endian length prefix followed by
// the row bytes — the first two fields of an HBase cell key. This prefix is
// accepted by hfile.Scanner.Seek to position at the first cell of a row.
func rowSeekKey(row []byte) []byte {
	key := make([]byte, 2+len(row))
	binary.BigEndian.PutUint16(key, uint16(len(row)))
	copy(key[2:], row)
	return key
}

// Next advances to the next visible cell, applying tombstone suppression and
// version limiting. Returns false when the stream is exhausted or on error.
func (s *RegionScanner) Next() bool {
	for {
		cell := s.heap.peek()
		if cell == nil {
			if err := s.heap.err(); err != nil {
				s.err = err
			}
			return false
		}

		// Advance the heap past this cell. The cell pointer remains valid
		// because each hfile.Scanner.Next() allocates a fresh *Cell.
		s.heap.next()

		// Stop when we reach or pass the end key.
		if len(s.opts.EndKey) > 0 && bytes.Compare(cell.Row, s.opts.EndKey) >= 0 {
			return false
		}

		// On row change, reset per-row tracking.
		if !bytes.Equal(cell.Row, s.currentRow) {
			s.currentRow = append(s.currentRow[:0], cell.Row...)
			s.deletes.reset()
			s.versionCounts = make(map[string]int)
		}

		// Tombstone: register and skip.
		if cell.Type != hfile.CellTypePut {
			s.deletes.add(cell)
			continue
		}

		// Put: check delete suppression.
		if s.deletes.isDeleted(cell) != NotDeleted {
			continue
		}

		// Put: check version limit.
		if s.opts.MaxVersions > 0 {
			qual := string(cell.Qualifier)
			if s.versionCounts[qual] >= s.opts.MaxVersions {
				continue
			}
			s.versionCounts[qual]++
		}

		s.cell = cell
		return true
	}
}

// Cell returns the current cell. Only valid after a successful Next() call.
func (s *RegionScanner) Cell() *hfile.Cell { return s.cell }

// Err returns any error encountered during scanning.
func (s *RegionScanner) Err() error { return s.err }
