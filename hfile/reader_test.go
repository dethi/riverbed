package hfile

import (
	"encoding/binary"
	"fmt"
	"os"
	"testing"
)

// makeCellKey builds a raw HBase cell key for use with Scanner.Seek.
// makeCellKey builds a full raw HBase cell key.
func makeCellKey(row, family, qualifier string, timestamp uint64, cellType CellType) []byte {
	keyLen := 2 + len(row) + 1 + len(family) + len(qualifier) + 8 + 1
	key := make([]byte, keyLen)
	off := 0
	binary.BigEndian.PutUint16(key[off:], uint16(len(row)))
	off += 2
	off += copy(key[off:], row)
	key[off] = byte(len(family))
	off++
	off += copy(key[off:], family)
	off += copy(key[off:], qualifier)
	binary.BigEndian.PutUint64(key[off:], timestamp)
	off += 8
	key[off] = byte(cellType)
	return key
}

// makeRowKey builds a partial key containing only rowLen + row.
func makeRowKey(row string) []byte {
	key := make([]byte, 2+len(row))
	binary.BigEndian.PutUint16(key, uint16(len(row)))
	copy(key[2:], row)
	return key
}

// makeRowFamilyQualifierKey builds a partial key containing rowLen + row +
// familyLen + family + qualifier.
func makeRowFamilyQualifierKey(row, family, qualifier string) []byte {
	key := make([]byte, 2+len(row)+1+len(family)+len(qualifier))
	off := 0
	binary.BigEndian.PutUint16(key[off:], uint16(len(row)))
	off += 2
	off += copy(key[off:], row)
	key[off] = byte(len(family))
	off++
	off += copy(key[off:], family)
	copy(key[off:], qualifier)
	return key
}

func TestReadSimpleHFile(t *testing.T) {
	f, err := os.Open("../testdata/simple.hfile")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}

	rd, err := Open(f, fi.Size())
	if err != nil {
		t.Fatal(err)
	}

	// Verify trailer fields.
	trailer := rd.Trailer()
	if trailer.MajorVersion != 3 {
		t.Errorf("major version = %d, want 3", trailer.MajorVersion)
	}
	if trailer.EntryCount != 10 {
		t.Errorf("entry count = %d, want 10", trailer.EntryCount)
	}
	if trailer.CompressionCodec != CompressionNone {
		t.Errorf("compression codec = %d, want %d (NONE)", trailer.CompressionCodec, CompressionNone)
	}
	if trailer.NumDataIndexLevels != 1 {
		t.Errorf("data index levels = %d, want 1", trailer.NumDataIndexLevels)
	}

	// Verify file info has expected keys.
	fileInfo := rd.FileInfo()
	for _, key := range []string{FileInfoAvgKeyLen, FileInfoAvgValueLen} {
		if _, ok := fileInfo[key]; !ok {
			t.Errorf("file info missing key %q", key)
		}
	}

	// Verify avg key/value lengths.
	if v, ok := fileInfo[FileInfoAvgKeyLen]; ok && len(v) == 4 {
		avgKeyLen := binary.BigEndian.Uint32(v)
		if avgKeyLen != 21 {
			t.Errorf("avg key len = %d, want 21", avgKeyLen)
		}
	}
	if v, ok := fileInfo[FileInfoAvgValueLen]; ok && len(v) == 4 {
		avgValLen := binary.BigEndian.Uint32(v)
		if avgValLen != 6 {
			t.Errorf("avg value len = %d, want 6", avgValLen)
		}
	}

	// Scan all cells and verify content.
	scanner := rd.Scanner()
	count := 0
	for scanner.Next() {
		c := scanner.Cell()
		row := fmt.Sprintf("row-%02d", count)
		val := fmt.Sprintf("val-%02d", count)

		if string(c.Row) != row {
			t.Errorf("cell %d: row = %q, want %q", count, c.Row, row)
		}
		if string(c.Family) != "cf" {
			t.Errorf("cell %d: family = %q, want %q", count, c.Family, "cf")
		}
		if string(c.Qualifier) != "q" {
			t.Errorf("cell %d: qualifier = %q, want %q", count, c.Qualifier, "q")
		}
		if c.Timestamp != 1700000000000 {
			t.Errorf("cell %d: timestamp = %d, want %d", count, c.Timestamp, 1700000000000)
		}
		if c.Type != CellTypePut {
			t.Errorf("cell %d: type = %d, want %d (Put)", count, c.Type, CellTypePut)
		}
		if string(c.Value) != val {
			t.Errorf("cell %d: value = %q, want %q", count, c.Value, val)
		}
		count++
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scanner error: %v", err)
	}
	if count != 10 {
		t.Errorf("cell count = %d, want 10", count)
	}
}

func openTestHFile(t *testing.T, path string) *Reader {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })

	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}

	rd, err := Open(f, fi.Size())
	if err != nil {
		t.Fatal(err)
	}
	return rd
}

func TestSeek(t *testing.T) {
	// seek.hfile has 100 cells: row-000..row-099, family=cf, qualifier=q,
	// timestamp=1700000000000, type=Put, value=val-000..val-099.
	// Small block size (256 bytes) ensures multiple data blocks.
	rd := openTestHFile(t, "testdata/seek.hfile")

	if rd.Trailer().NumDataIndexLevels < 1 {
		t.Fatalf("expected multiple data blocks, got %d index levels", rd.Trailer().NumDataIndexLevels)
	}

	const (
		family    = "cf"
		qualifier = "q"
		timestamp = uint64(1700000000000)
	)

	t.Run("ExactFirst", func(t *testing.T) {
		scanner := rd.Scanner()
		key := makeCellKey("row-000", family, qualifier, timestamp, CellTypePut)
		if !scanner.Seek(key) {
			t.Fatalf("Seek returned false, err: %v", scanner.Err())
		}
		c := scanner.Cell()
		if string(c.Row) != "row-000" {
			t.Errorf("row = %q, want %q", c.Row, "row-000")
		}
		if string(c.Value) != "val-000" {
			t.Errorf("value = %q, want %q", c.Value, "val-000")
		}
	})

	t.Run("ExactMiddle", func(t *testing.T) {
		scanner := rd.Scanner()
		key := makeCellKey("row-050", family, qualifier, timestamp, CellTypePut)
		if !scanner.Seek(key) {
			t.Fatalf("Seek returned false, err: %v", scanner.Err())
		}
		c := scanner.Cell()
		if string(c.Row) != "row-050" {
			t.Errorf("row = %q, want %q", c.Row, "row-050")
		}
		if string(c.Value) != "val-050" {
			t.Errorf("value = %q, want %q", c.Value, "val-050")
		}
	})

	t.Run("ExactLast", func(t *testing.T) {
		scanner := rd.Scanner()
		key := makeCellKey("row-099", family, qualifier, timestamp, CellTypePut)
		if !scanner.Seek(key) {
			t.Fatalf("Seek returned false, err: %v", scanner.Err())
		}
		c := scanner.Cell()
		if string(c.Row) != "row-099" {
			t.Errorf("row = %q, want %q", c.Row, "row-099")
		}
		if string(c.Value) != "val-099" {
			t.Errorf("value = %q, want %q", c.Value, "val-099")
		}
	})

	t.Run("BeforeAll", func(t *testing.T) {
		scanner := rd.Scanner()
		key := makeCellKey("aaa", family, qualifier, timestamp, CellTypePut)
		if !scanner.Seek(key) {
			t.Fatalf("Seek returned false, err: %v", scanner.Err())
		}
		c := scanner.Cell()
		if string(c.Row) != "row-000" {
			t.Errorf("row = %q, want %q", c.Row, "row-000")
		}
	})

	t.Run("AfterAll", func(t *testing.T) {
		// "row-100" has the same length as existing rows but sorts after "row-099".
		scanner := rd.Scanner()
		key := makeCellKey("row-100", family, qualifier, timestamp, CellTypePut)
		if scanner.Seek(key) {
			t.Fatalf("Seek returned true, want false (key after all cells)")
		}
		if err := scanner.Err(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("BetweenRows", func(t *testing.T) {
		// Use row-042 with a very high timestamp to create a key that sorts
		// after the existing row-042 cell but before row-043 in raw byte order.
		scanner := rd.Scanner()
		key := makeCellKey("row-042", family, qualifier, ^uint64(0), CellTypePut)
		if !scanner.Seek(key) {
			t.Fatalf("Seek returned false, err: %v", scanner.Err())
		}
		c := scanner.Cell()
		if string(c.Row) != "row-043" {
			t.Errorf("row = %q, want %q", c.Row, "row-043")
		}
	})

	t.Run("NextAfterSeek", func(t *testing.T) {
		// Seek then continue scanning with Next.
		scanner := rd.Scanner()
		key := makeCellKey("row-097", family, qualifier, timestamp, CellTypePut)
		if !scanner.Seek(key) {
			t.Fatalf("Seek returned false, err: %v", scanner.Err())
		}
		c := scanner.Cell()
		if string(c.Row) != "row-097" {
			t.Errorf("row = %q, want %q", c.Row, "row-097")
		}

		// Should be able to get row-098 and row-099 via Next.
		for _, want := range []string{"row-098", "row-099"} {
			if !scanner.Next() {
				t.Fatalf("Next returned false at %s, err: %v", want, scanner.Err())
			}
			if string(scanner.Cell().Row) != want {
				t.Errorf("row = %q, want %q", scanner.Cell().Row, want)
			}
		}

		// No more cells.
		if scanner.Next() {
			t.Errorf("Next returned true, want false after last cell")
		}
	})

	t.Run("MultipleSeeks", func(t *testing.T) {
		// Seek forward, then seek backward.
		scanner := rd.Scanner()

		key := makeCellKey("row-080", family, qualifier, timestamp, CellTypePut)
		if !scanner.Seek(key) {
			t.Fatalf("first Seek returned false, err: %v", scanner.Err())
		}
		if string(scanner.Cell().Row) != "row-080" {
			t.Errorf("row = %q, want %q", scanner.Cell().Row, "row-080")
		}

		// Seek backward to an earlier row.
		key = makeCellKey("row-010", family, qualifier, timestamp, CellTypePut)
		if !scanner.Seek(key) {
			t.Fatalf("second Seek returned false, err: %v", scanner.Err())
		}
		if string(scanner.Cell().Row) != "row-010" {
			t.Errorf("row = %q, want %q", scanner.Cell().Row, "row-010")
		}
	})

	t.Run("SeekAfterScan", func(t *testing.T) {
		// Scan a few cells, then seek to a specific row.
		scanner := rd.Scanner()
		for i := range 5 {
			if !scanner.Next() {
				t.Fatalf("Next returned false at %d, err: %v", i, scanner.Err())
			}
		}

		key := makeCellKey("row-060", family, qualifier, timestamp, CellTypePut)
		if !scanner.Seek(key) {
			t.Fatalf("Seek returned false, err: %v", scanner.Err())
		}
		if string(scanner.Cell().Row) != "row-060" {
			t.Errorf("row = %q, want %q", scanner.Cell().Row, "row-060")
		}
	})

	t.Run("PartialKeyRowOnly", func(t *testing.T) {
		scanner := rd.Scanner()
		key := makeRowKey("row-050")
		if !scanner.Seek(key) {
			t.Fatalf("Seek returned false, err: %v", scanner.Err())
		}
		if string(scanner.Cell().Row) != "row-050" {
			t.Errorf("row = %q, want %q", scanner.Cell().Row, "row-050")
		}
	})

	t.Run("PartialKeyRowFamilyQualifier", func(t *testing.T) {
		scanner := rd.Scanner()
		key := makeRowFamilyQualifierKey("row-075", family, qualifier)
		if !scanner.Seek(key) {
			t.Fatalf("Seek returned false, err: %v", scanner.Err())
		}
		if string(scanner.Cell().Row) != "row-075" {
			t.Errorf("row = %q, want %q", scanner.Cell().Row, "row-075")
		}
	})

	t.Run("PartialKeyRowOnlyBeforeAll", func(t *testing.T) {
		scanner := rd.Scanner()
		key := makeRowKey("aaa-000")
		if !scanner.Seek(key) {
			t.Fatalf("Seek returned false, err: %v", scanner.Err())
		}
		if string(scanner.Cell().Row) != "row-000" {
			t.Errorf("row = %q, want %q", scanner.Cell().Row, "row-000")
		}
	})

	t.Run("PartialKeyRowOnlyAfterAll", func(t *testing.T) {
		// "row-100" has same length as existing rows but sorts after all of them.
		scanner := rd.Scanner()
		key := makeRowKey("row-100")
		if scanner.Seek(key) {
			t.Fatalf("Seek returned true, want false (partial key after all cells)")
		}
		if err := scanner.Err(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}
