package hfile

import (
	"encoding/binary"
	"fmt"
)

// CellType represents the type of a cell operation.
type CellType byte

const (
	CellTypePut          CellType = 4
	CellTypeDelete       CellType = 8
	CellTypeDeleteColumn CellType = 12
	CellTypeDeleteFamily CellType = 14
)

func (t CellType) String() string {
	switch t {
	case CellTypePut:
		return "Put"
	case CellTypeDelete:
		return "Delete"
	case CellTypeDeleteColumn:
		return "DeleteColumn"
	case CellTypeDeleteFamily:
		return "DeleteFamily"
	default:
		return fmt.Sprintf("Unknown(%d)", t)
	}
}

// Cell represents a single HBase cell (KeyValue).
type Cell struct {
	Row       []byte
	Family    []byte
	Qualifier []byte
	Timestamp uint64
	Type      CellType
	Value     []byte
	Tags      []byte
}

// CellIterator iterates over cells in a data block without allocating a slice upfront.
type CellIterator struct {
	data        []byte
	offset      int
	includeTags bool
	cell        *Cell
	err         error
}

// NewCellIterator creates an iterator over cells in a data block payload.
// includeTags indicates whether cells contain a 2-byte tags length + tags data.
func NewCellIterator(data []byte, includeTags bool) *CellIterator {
	return &CellIterator{data: data, includeTags: includeTags}
}

// Next advances to the next cell. Returns false when done or on error.
func (it *CellIterator) Next() bool {
	if it.offset >= len(it.data) || it.err != nil {
		return false
	}
	cell, n, err := parseCell(it.data, it.offset, it.includeTags)
	if err != nil {
		it.err = err
		return false
	}
	it.cell = cell
	it.offset += n
	return true
}

// Cell returns the current cell.
func (it *CellIterator) Cell() *Cell { return it.cell }

// Err returns any error encountered during iteration.
func (it *CellIterator) Err() error { return it.err }

func parseCell(data []byte, offset int, includeTags bool) (*Cell, int, error) {
	if offset+8 > len(data) {
		return nil, 0, fmt.Errorf("hfile: cell at offset %d: not enough data for key/value lengths", offset)
	}

	keyLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
	valLen := int(binary.BigEndian.Uint32(data[offset+4 : offset+8]))
	pos := offset + 8

	if pos+keyLen+valLen > len(data) {
		return nil, 0, fmt.Errorf("hfile: cell at offset %d: data too short for key(%d)+value(%d)", offset, keyLen, valLen)
	}

	// Parse key region.
	keyStart := pos
	if keyLen < 2+1+8+1 { // minimum: rowLen(2) + familyLen(1) + timestamp(8) + type(1) = 12
		return nil, 0, fmt.Errorf("hfile: cell at offset %d: key too short (%d bytes)", offset, keyLen)
	}

	rowLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2
	if pos+rowLen > keyStart+keyLen {
		return nil, 0, fmt.Errorf("hfile: cell at offset %d: row extends past key", offset)
	}
	row := data[pos : pos+rowLen]
	pos += rowLen

	familyLen := int(data[pos])
	pos++
	if pos+familyLen > keyStart+keyLen {
		return nil, 0, fmt.Errorf("hfile: cell at offset %d: family extends past key", offset)
	}
	family := data[pos : pos+familyLen]
	pos += familyLen

	// qualifier = remaining key bytes minus timestamp(8) + type(1)
	qualLen := keyLen - (2 + rowLen + 1 + familyLen + 8 + 1)
	if qualLen < 0 {
		return nil, 0, fmt.Errorf("hfile: cell at offset %d: negative qualifier length", offset)
	}
	qualifier := data[pos : pos+qualLen]
	pos += qualLen

	timestamp := binary.BigEndian.Uint64(data[pos : pos+8])
	pos += 8

	cellType := CellType(data[pos])
	pos++

	// Value.
	value := data[pos : pos+valLen]
	pos += valLen

	// Tags (v3): present only when the HFile includes tags (MAX_TAGS_LEN > 0).
	var tags []byte
	if includeTags && pos+2 <= len(data) {
		tagsLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
		pos += 2
		if tagsLen > 0 {
			if pos+tagsLen > len(data) {
				return nil, 0, fmt.Errorf("hfile: cell at offset %d: tags extend past data", offset)
			}
			tags = data[pos : pos+tagsLen]
			pos += tagsLen
		}
	}

	// HFile v3 appends a memstoreTS (Hadoop VInt) after each cell.
	if pos < len(data) {
		_, n, err := readVInt(data, pos)
		if err != nil {
			return nil, 0, fmt.Errorf("hfile: cell at offset %d: read memstoreTS: %w", offset, err)
		}
		pos += n
	}

	return &Cell{
		Row:       row,
		Family:    family,
		Qualifier: qualifier,
		Timestamp: timestamp,
		Type:      cellType,
		Value:     value,
		Tags:      tags,
	}, pos - offset, nil
}
