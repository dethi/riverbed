package hfile

import (
	"encoding/binary"
	"fmt"
	"os"
	"testing"
)

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
