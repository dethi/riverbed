package hfile

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"pgregory.net/rapid"
)

// Recipe types matching the Java side.

type cellTemplate struct {
	QualifierSize int   `json:"qualifierSize"`
	ValueSize     int   `json:"valueSize"`
	Timestamp     int64 `json:"timestamp"`
	Type          int   `json:"type"`
}

type rowGroup struct {
	RowCount   int            `json:"rowCount"`
	RowKeySize int            `json:"rowKeySize"`
	Cells      []cellTemplate `json:"cells"`
}

type recipe struct {
	OutputPath  string     `json:"outputPath"`
	Compression string     `json:"compression"`
	BlockSize   int        `json:"blockSize"`
	Seed        int64      `json:"seed"`
	Family      string     `json:"family"`
	Groups      []rowGroup `json:"groups"`
}

// Rapid generators.

func genRecipe(t *rapid.T) recipe {
	seed := rapid.Int64().Draw(t, "seed")
	compression := rapid.SampledFrom([]string{"NONE", "GZ", "SNAPPY", "ZSTD"}).Draw(t, "compression")
	blockSize := rapid.IntRange(64, 65536).Draw(t, "blockSize")
	family := rapid.StringMatching(`[a-z]{1,5}`).Draw(t, "family")

	nGroups := rapid.IntRange(1, 10).Draw(t, "nGroups")
	groups := make([]rowGroup, nGroups)
	for g := range groups {
		groups[g] = genRowGroup(t)
	}

	return recipe{
		Seed:        seed,
		Compression: compression,
		BlockSize:   blockSize,
		Family:      family,
		Groups:      groups,
	}
}

func genRowGroup(t *rapid.T) rowGroup {
	rowCount := rapid.IntRange(1, 5000).Draw(t, "rowCount")
	rowKeySize := rapid.IntRange(4, 100).Draw(t, "rowKeySize")

	nCells := rapid.IntRange(1, 20).Draw(t, "cellsPerRow")
	cells := make([]cellTemplate, nCells)
	for i := range cells {
		cells[i] = cellTemplate{
			QualifierSize: rapid.IntRange(0, 50).Draw(t, "qualifierSize"),
			ValueSize:     rapid.IntRange(0, 10000).Draw(t, "valueSize"),
			Timestamp:     rapid.Int64Range(1, 2000000000000).Draw(t, "timestamp"),
			Type:          rapid.SampledFrom([]int{4, 8}).Draw(t, "type"),
		}
	}
	return rowGroup{RowCount: rowCount, RowKeySize: rowKeySize, Cells: cells}
}

// Deterministic expansion (must match Java).

func longToLE(v int64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(v))
	return buf
}

func generateRowKey(seed int64, globalIdx, size int) []byte {
	key := make([]byte, size)
	binary.BigEndian.PutUint32(key[:4], uint32(globalIdx))
	if size > 4 {
		h := sha256.New()
		h.Write(longToLE(seed))
		h.Write(longToLE(int64(globalIdx)))
		hash := h.Sum(nil)
		copy(key[4:], hash)
	}
	return key
}

func generateQualifier(seed int64, globalIdx, cellIdx, size int) []byte {
	if size == 0 {
		return nil
	}
	h := sha256.New()
	h.Write(longToLE(seed))
	h.Write(longToLE(int64(globalIdx)))
	h.Write(longToLE(int64(cellIdx)))
	h.Write([]byte{'q'})
	hash := h.Sum(nil)
	if size > len(hash) {
		size = len(hash)
	}
	return hash[:size]
}

func generateRecipeValue(seed int64, globalIdx, cellIdx, size int) []byte {
	if size == 0 {
		return nil
	}
	h := sha256.New()
	h.Write(longToLE(seed))
	h.Write(longToLE(int64(globalIdx)))
	h.Write(longToLE(int64(cellIdx)))
	h.Write([]byte{'v'})
	block := h.Sum(nil)
	result := make([]byte, size)
	off := 0
	for off < size {
		n := copy(result[off:], block)
		off += n
	}
	return result
}

// hbaseCellLess returns true if a should be ordered before b in HBase order.
// Within an HFile (single family), the order is: row ASC, qualifier ASC, timestamp DESC, type DESC.
func hbaseCellLess(a, b *Cell) bool {
	if cmp := bytes.Compare(a.Row, b.Row); cmp != 0 {
		return cmp < 0
	}
	if cmp := bytes.Compare(a.Qualifier, b.Qualifier); cmp != 0 {
		return cmp < 0
	}
	if a.Timestamp != b.Timestamp {
		return a.Timestamp > b.Timestamp // DESC
	}
	return a.Type > b.Type // DESC
}

func hbaseCellEqual(a, b *Cell) bool {
	return bytes.Equal(a.Row, b.Row) &&
		bytes.Equal(a.Qualifier, b.Qualifier) &&
		a.Timestamp == b.Timestamp &&
		a.Type == b.Type
}

// expandRecipe expands a recipe into sorted, deduplicated cells.
func expandRecipe(r recipe) []Cell {
	family := []byte(r.Family)

	var all []Cell
	globalIdx := 0
	for _, group := range r.Groups {
		for ri := 0; ri < group.RowCount; ri++ {
			rowKey := generateRowKey(r.Seed, globalIdx, group.RowKeySize)

			var rowCells []Cell
			for ci, ct := range group.Cells {
				rowCells = append(rowCells, Cell{
					Row:       rowKey,
					Family:    family,
					Qualifier: generateQualifier(r.Seed, globalIdx, ci, ct.QualifierSize),
					Timestamp: uint64(ct.Timestamp),
					Type:      CellType(ct.Type),
					Value:     generateRecipeValue(r.Seed, globalIdx, ci, ct.ValueSize),
				})
			}

			// Sort by HBase order within row (stable to match Java's List.sort).
			slices.SortStableFunc(rowCells, func(a, b Cell) int {
				if hbaseCellLess(&a, &b) {
					return -1
				}
				if hbaseCellLess(&b, &a) {
					return 1
				}
				return 0
			})

			// Deduplicate.
			deduped := rowCells[:0]
			for i := range rowCells {
				if i > 0 && hbaseCellEqual(&rowCells[i], &rowCells[i-1]) {
					continue
				}
				deduped = append(deduped, rowCells[i])
			}

			all = append(all, deduped...)
			globalIdx++
		}
	}
	return all
}

const maxRecipeBytes = 128 << 20 // 128 MB

func estimateRecipeSize(r recipe) int64 {
	var total int64
	famSize := int64(len(r.Family))
	for _, g := range r.Groups {
		var perRow int64
		for _, c := range g.Cells {
			perRow += int64(g.RowKeySize) + famSize + int64(c.QualifierSize+c.ValueSize+32)
		}
		total += int64(g.RowCount) * perRow
	}
	return total
}

func TestHFileProperties(t *testing.T) {
	mvnOnce.Do(checkMvn)
	if !mvnAvailable {
		t.Skip("mvn not available or compilation failed")
	}

	srv, err := getServer()
	if err != nil {
		t.Fatalf("start server: %v", err)
	}

	t.Logf("%s: tmp=%s", t.Name(), t.TempDir())

	rapid.Check(t, func(rt *rapid.T) {
		r := genRecipe(rt)

		if estimateRecipeSize(r) > maxRecipeBytes {
			rt.Skip("recipe too large")
		}

		expectedCells := expandRecipe(r)
		if len(expectedCells) == 0 {
			rt.Skip("no cells")
		}

		r.OutputPath = filepath.Join(t.TempDir(), fmt.Sprintf("test-%d.hfile", rapid.Int64().Draw(rt, "fileid")))

		if err := srv.generateRaw(r); err != nil {
			rt.Fatalf("generate hfile: %v", err)
		}

		verifyHFileProperties(rt, r.OutputPath, expectedCells)
	})
}

func verifyHFileProperties(t *rapid.T, path string, expectedCells []Cell) {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open hfile: %v", err)
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		t.Fatalf("stat hfile: %v", err)
	}

	rd, err := Open(file, fi.Size())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Skip multi-level indexes.
	if rd.DataIndex().NumDataIndexLevels > 1 {
		t.Skip("multi-level index not yet supported")
	}

	// Property 1: EntryCount matches.
	if got := int(rd.Trailer().EntryCount); got != len(expectedCells) {
		t.Fatalf("EntryCount = %d, want %d", got, len(expectedCells))
	}

	scanner := rd.Scanner()
	count := 0
	var prev *Cell

	for scanner.Next() {
		c := scanner.Cell()

		if count >= len(expectedCells) {
			t.Fatalf("scanner returned more cells than expected (%d)", len(expectedCells))
		}

		exp := &expectedCells[count]

		// Property 2: Cell content matches.
		if !bytes.Equal(c.Row, exp.Row) {
			t.Errorf("cell %d: row mismatch: got %x, want %x", count, c.Row, exp.Row)
		}
		if !bytes.Equal(c.Family, exp.Family) {
			t.Errorf("cell %d: family = %q, want %q", count, c.Family, exp.Family)
		}
		if !bytes.Equal(c.Qualifier, exp.Qualifier) {
			t.Errorf("cell %d: qualifier mismatch: got %x, want %x", count, c.Qualifier, exp.Qualifier)
		}
		if c.Timestamp != exp.Timestamp {
			t.Errorf("cell %d: timestamp = %d, want %d", count, c.Timestamp, exp.Timestamp)
		}
		if c.Type != exp.Type {
			t.Errorf("cell %d: type = %v, want %v", count, c.Type, exp.Type)
		}
		if !bytes.Equal(c.Value, exp.Value) {
			t.Errorf("cell %d: value mismatch (len got=%d want=%d)", count, len(c.Value), len(exp.Value))
		}

		// Property 3: HBase sort order.
		if prev != nil && !hbaseCellLess(prev, c) && !hbaseCellEqual(prev, c) {
			t.Errorf("cell %d: not in HBase order relative to previous", count)
		}
		cellCopy := *c
		prev = &cellCopy

		count++
	}

	// Property 4: No scanner error.
	if err := scanner.Err(); err != nil {
		t.Fatalf("scanner error: %v", err)
	}

	// Property 5: Completeness.
	if count != len(expectedCells) {
		t.Errorf("scanned %d cells, want %d", count, len(expectedCells))
	}
}
