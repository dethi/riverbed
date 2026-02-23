package scanner

import "github.com/dethi/riverbed/hfile"

// DeleteStatus indicates why a Put cell is suppressed.
type DeleteStatus int

const (
	NotDeleted     DeleteStatus = iota
	FamilyDeleted               // suppressed by a DELETE_FAMILY marker
	ColumnDeleted               // suppressed by a DELETE_COLUMN marker
	VersionDeleted              // suppressed by a DELETE (point-delete) marker
)

// deleteTracker tracks tombstones within a single row.
// It must be reset at the start of each new row.
// Mirrors HBase ScanDeleteTracker.
type deleteTracker struct {
	// familyDeleteTS is the maximum timestamp seen in a DELETE_FAMILY marker.
	// Any Put with Timestamp <= familyDeleteTS is suppressed.
	// Zero means no DELETE_FAMILY has been seen.
	familyDeleteTS uint64

	// columnDeleteTS maps qualifier → maximum DELETE_COLUMN timestamp.
	// A Put is suppressed if its Timestamp <= columnDeleteTS[qualifier].
	columnDeleteTS map[string]uint64

	// versionDeletes maps qualifier → set of timestamps deleted by DELETE markers.
	versionDeletes map[string]map[uint64]struct{}
}

func newDeleteTracker() *deleteTracker {
	return &deleteTracker{
		columnDeleteTS: make(map[string]uint64),
		versionDeletes: make(map[string]map[uint64]struct{}),
	}
}

// reset clears all state; call at the start of each new row.
func (dt *deleteTracker) reset() {
	dt.familyDeleteTS = 0
	dt.columnDeleteTS = make(map[string]uint64)
	dt.versionDeletes = make(map[string]map[uint64]struct{})
}

// add registers a tombstone cell. Call for every non-Put cell.
func (dt *deleteTracker) add(cell *hfile.Cell) {
	qual := string(cell.Qualifier)
	switch cell.Type {
	case hfile.CellTypeDeleteFamily:
		if cell.Timestamp > dt.familyDeleteTS {
			dt.familyDeleteTS = cell.Timestamp
		}
	case hfile.CellTypeDeleteColumn:
		if cell.Timestamp > dt.columnDeleteTS[qual] {
			dt.columnDeleteTS[qual] = cell.Timestamp
		}
	case hfile.CellTypeDelete:
		if dt.versionDeletes[qual] == nil {
			dt.versionDeletes[qual] = make(map[uint64]struct{})
		}
		dt.versionDeletes[qual][cell.Timestamp] = struct{}{}
	}
}

// isDeleted reports whether a Put cell is suppressed by a previously seen tombstone.
func (dt *deleteTracker) isDeleted(cell *hfile.Cell) DeleteStatus {
	qual := string(cell.Qualifier)

	if dt.familyDeleteTS > 0 && cell.Timestamp <= dt.familyDeleteTS {
		return FamilyDeleted
	}
	if ts, ok := dt.columnDeleteTS[qual]; ok && cell.Timestamp <= ts {
		return ColumnDeleted
	}
	if versions, ok := dt.versionDeletes[qual]; ok {
		if _, deleted := versions[cell.Timestamp]; deleted {
			return VersionDeleted
		}
	}
	return NotDeleted
}
