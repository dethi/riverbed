package scanner

import (
	"bytes"

	"github.com/dethi/riverbed/hfile"
)

// CompareCell returns negative/0/positive for a<b, a==b, a>b using HBase
// CellComparatorImpl ordering:
//
//	Row       ASC
//	Family    ASC
//	Qualifier ASC
//	Timestamp DESC (newer first)
//	Type      DESC (DeleteFamily=14 > DeleteColumn=12 > Delete=8 > Put=4)
func CompareCell(a, b *hfile.Cell) int {
	if cmp := bytes.Compare(a.Row, b.Row); cmp != 0 {
		return cmp
	}
	if cmp := bytes.Compare(a.Family, b.Family); cmp != 0 {
		return cmp
	}
	if cmp := bytes.Compare(a.Qualifier, b.Qualifier); cmp != 0 {
		return cmp
	}
	// Timestamp descending: higher timestamp = smaller order.
	if a.Timestamp != b.Timestamp {
		if a.Timestamp > b.Timestamp {
			return -1
		}
		return 1
	}
	// Type descending: higher type value = smaller order.
	if a.Type != b.Type {
		if a.Type > b.Type {
			return -1
		}
		return 1
	}
	return 0
}

// CompareCellWithOrder extends CompareCell with a tie-breaking scanner order:
// lower aOrder means higher priority (newer file).
func CompareCellWithOrder(a, b *hfile.Cell, aOrder, bOrder int64) int {
	if cmp := CompareCell(a, b); cmp != 0 {
		return cmp
	}
	if aOrder < bOrder {
		return -1
	}
	if aOrder > bOrder {
		return 1
	}
	return 0
}
