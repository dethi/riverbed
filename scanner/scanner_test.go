package scanner

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/dethi/riverbed/hfile"
	hfilepb "github.com/dethi/riverbed/hfile/proto"
	"google.golang.org/protobuf/proto"
)

// ---------------------------------------------------------------------------
// Minimal in-memory HFile builder (NONE compression, no checksums, 1 data block)
// ---------------------------------------------------------------------------

// Block magic bytes (unexported in hfile package, duplicated here).
var (
	testMagicData     = [8]byte{'D', 'A', 'T', 'A', 'B', 'L', 'K', '*'}
	testMagicRootIdx  = [8]byte{'I', 'D', 'X', 'R', 'O', 'O', 'T', '2'}
	testMagicFileInfo = [8]byte{'F', 'I', 'L', 'E', 'I', 'N', 'F', '2'}
)

const testBlockHeaderSize = 33 // 8+4+4+8+1+4+4

// testWriteBlock appends an HFile block (header + payload) to buf.
// Uses NONE compression and checksumType=0 (null).
func testWriteBlock(buf *bytes.Buffer, magic [8]byte, payload []byte) {
	n := len(payload)
	var hdr [testBlockHeaderSize]byte
	copy(hdr[0:8], magic[:])
	binary.BigEndian.PutUint32(hdr[8:12], uint32(n))  // onDiskSizeWithoutHeader
	binary.BigEndian.PutUint32(hdr[12:16], uint32(n)) // uncompressedSize
	// prevBlockOffset at [16:24] stays 0
	// checksumType at [24] stays 0 (null)
	// bytesPerChecksum at [25:29] stays 0
	binary.BigEndian.PutUint32(hdr[29:33], uint32(testBlockHeaderSize+n)) // onDiskDataSizeWithHeader
	buf.Write(hdr[:])
	buf.Write(payload)
}

// testEncodeCells serialises cells in HBase cell wire format:
//
//	keyLen(4BE) + valLen(4BE) + key + value + memstoreTS(0x00)
//
// where key = rowLen(2BE) + row + familyLen(1) + family + qualifier + ts(8BE) + type(1).
func testEncodeCells(cells []hfile.Cell) []byte {
	var buf bytes.Buffer
	for _, c := range cells {
		keyLen := 2 + len(c.Row) + 1 + len(c.Family) + len(c.Qualifier) + 8 + 1
		// cell header
		var cell [8]byte
		binary.BigEndian.PutUint32(cell[0:4], uint32(keyLen))
		binary.BigEndian.PutUint32(cell[4:8], uint32(len(c.Value)))
		buf.Write(cell[:])
		// key
		var rowLen [2]byte
		binary.BigEndian.PutUint16(rowLen[:], uint16(len(c.Row)))
		buf.Write(rowLen[:])
		buf.Write(c.Row)
		buf.WriteByte(byte(len(c.Family)))
		buf.Write(c.Family)
		buf.Write(c.Qualifier)
		var ts [8]byte
		binary.BigEndian.PutUint64(ts[:], c.Timestamp)
		buf.Write(ts[:])
		buf.WriteByte(byte(c.Type))
		// value
		buf.Write(c.Value)
		// memstoreTS = 0 (Hadoop VInt: single byte 0x00 for values in [-112, 127])
		buf.WriteByte(0x00)
	}
	return buf.Bytes()
}

// testCellKey returns the raw HBase key bytes for a cell (for root index entries).
func testCellKey(c *hfile.Cell) []byte {
	key := make([]byte, 2+len(c.Row)+1+len(c.Family)+len(c.Qualifier)+8+1)
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

// testBuildTrailer returns a 4096-byte HFile v3 trailer.
func testBuildTrailer(fileInfoOffset, loadOnOpenOffset, entryCount, firstDataOff, lastDataOff uint64, dataIndexCount uint32) []byte {
	codec := uint32(hfile.CompressionNone)
	numLevels := uint32(1)
	comparator := "org.apache.hadoop.hbase.CellComparator"
	pb := &hfilepb.FileTrailerProto{
		FileInfoOffset:       &fileInfoOffset,
		LoadOnOpenDataOffset: &loadOnOpenOffset,
		EntryCount:           &entryCount,
		DataIndexCount:       &dataIndexCount,
		NumDataIndexLevels:   &numLevels,
		FirstDataBlockOffset: &firstDataOff,
		LastDataBlockOffset:  &lastDataOff,
		CompressionCodec:     &codec,
		ComparatorClassName:  &comparator,
	}
	pbBytes, err := proto.Marshal(pb)
	if err != nil {
		panic("marshal trailer: " + err.Error())
	}
	trailer := make([]byte, 4096)
	copy(trailer[0:8], []byte{'T', 'R', 'A', 'B', 'L', 'K', '"', '$'})
	off := 8
	off += binary.PutUvarint(trailer[off:], uint64(len(pbBytes)))
	copy(trailer[off:], pbBytes)
	// version bytes at [4092:4096]: (minorVersion<<24)|majorVersion = (3<<24)|3
	binary.BigEndian.PutUint32(trailer[4092:], 0x03000003)
	return trailer
}

// buildHFile creates a minimal valid HFile v3 binary from pre-sorted cells.
// All cells go into a single DATA block; NONE compression; no checksums.
func buildHFile(t *testing.T, cells []hfile.Cell) []byte {
	t.Helper()
	var buf bytes.Buffer

	cellData := testEncodeCells(cells)
	dataBlockOffset := int64(0)

	// 1. Single DATA block containing all cells.
	testWriteBlock(&buf, testMagicData, cellData)

	// 2. Root index block: one entry pointing at the data block.
	loadOnOpenOffset := int64(buf.Len())
	var rootIdx bytes.Buffer
	var offsetBuf [8]byte
	binary.BigEndian.PutUint64(offsetBuf[:], uint64(dataBlockOffset))
	rootIdx.Write(offsetBuf[:])
	var sizeBuf [4]byte
	binary.BigEndian.PutUint32(sizeBuf[:], uint32(len(cellData)))
	rootIdx.Write(sizeBuf[:])
	if len(cells) > 0 {
		k := testCellKey(&cells[0])
		rootIdx.WriteByte(byte(len(k))) // VInt: single byte for lengths ≤ 127
		rootIdx.Write(k)
	} else {
		rootIdx.WriteByte(0x00) // zero-length key
	}
	testWriteBlock(&buf, testMagicRootIdx, rootIdx.Bytes())

	// 3. Empty meta index block (ROOT_INDEX type, 0 bytes of payload).
	testWriteBlock(&buf, testMagicRootIdx, nil)

	// 4. File info block: "PBUF" magic + varint(0) for empty FileInfoProto.
	fileInfoOffset := int64(buf.Len())
	testWriteBlock(&buf, testMagicFileInfo, []byte{'P', 'B', 'U', 'F', 0x00})

	// 5. Trailer (4096 bytes at end of file).
	trailer := testBuildTrailer(
		uint64(fileInfoOffset),
		uint64(loadOnOpenOffset),
		uint64(len(cells)),
		uint64(dataBlockOffset),
		uint64(dataBlockOffset),
		1, // 1 data block → 1 root index entry
	)
	buf.Write(trailer)

	return buf.Bytes()
}

// openHFile opens an hfile.Reader from raw bytes.
func openHFile(t *testing.T, data []byte) *hfile.Reader {
	t.Helper()
	rd, err := hfile.Open(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("hfile.Open: %v", err)
	}
	return rd
}

// cell constructs a test hfile.Cell.
func cell(row, family, qualifier string, ts uint64, ct hfile.CellType, value string) hfile.Cell {
	return hfile.Cell{
		Row:       []byte(row),
		Family:    []byte(family),
		Qualifier: []byte(qualifier),
		Timestamp: ts,
		Type:      ct,
		Value:     []byte(value),
	}
}

// collect drains a RegionScanner into a slice of copied cells.
func collect(t *testing.T, rs *RegionScanner) []hfile.Cell {
	t.Helper()
	var out []hfile.Cell
	for rs.Next() {
		out = append(out, *rs.Cell()) // copy
	}
	if err := rs.Err(); err != nil {
		t.Fatalf("RegionScanner.Err(): %v", err)
	}
	return out
}

// newRS creates a RegionScanner from a single hfile.Reader.
func newRS(t *testing.T, rd *hfile.Reader, opts Options) *RegionScanner {
	t.Helper()
	rs, err := NewRegionScanner([]*hfile.Scanner{rd.Scanner()}, opts)
	if err != nil {
		t.Fatal(err)
	}
	return rs
}

// newRSMulti creates a RegionScanner from multiple hfile.Readers (newest first).
func newRSMulti(t *testing.T, readers []*hfile.Reader, opts Options) *RegionScanner {
	t.Helper()
	scanners := make([]*hfile.Scanner, len(readers))
	for i, rd := range readers {
		scanners[i] = rd.Scanner()
	}
	rs, err := NewRegionScanner(scanners, opts)
	if err != nil {
		t.Fatal(err)
	}
	return rs
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const fam = "cf"

func TestSingleScannerPassthrough(t *testing.T) {
	want := []hfile.Cell{
		cell("row1", fam, "q", 100, hfile.CellTypePut, "v1"),
		cell("row2", fam, "q", 100, hfile.CellTypePut, "v2"),
		cell("row3", fam, "q", 100, hfile.CellTypePut, "v3"),
	}
	rd := openHFile(t, buildHFile(t, want))
	got := collect(t, newRS(t, rd, Options{}))

	if len(got) != len(want) {
		t.Fatalf("got %d cells, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i].Row, want[i].Row) {
			t.Errorf("[%d] row = %q, want %q", i, got[i].Row, want[i].Row)
		}
		if !bytes.Equal(got[i].Value, want[i].Value) {
			t.Errorf("[%d] value = %q, want %q", i, got[i].Value, want[i].Value)
		}
	}
}

func TestTwoNonOverlappingFiles(t *testing.T) {
	// Newer file (order=0): row1, row3
	cellsA := []hfile.Cell{
		cell("row1", fam, "q", 100, hfile.CellTypePut, "a1"),
		cell("row3", fam, "q", 100, hfile.CellTypePut, "a3"),
	}
	// Older file (order=1): row2, row4
	cellsB := []hfile.Cell{
		cell("row2", fam, "q", 100, hfile.CellTypePut, "b2"),
		cell("row4", fam, "q", 100, hfile.CellTypePut, "b4"),
	}

	rdA := openHFile(t, buildHFile(t, cellsA))
	rdB := openHFile(t, buildHFile(t, cellsB))
	got := collect(t, newRSMulti(t, []*hfile.Reader{rdA, rdB}, Options{}))

	wantRows := []string{"row1", "row2", "row3", "row4"}
	if len(got) != len(wantRows) {
		t.Fatalf("got %d cells, want %d", len(got), len(wantRows))
	}
	for i, row := range wantRows {
		if string(got[i].Row) != row {
			t.Errorf("[%d] row = %q, want %q", i, got[i].Row, row)
		}
	}
}

func TestTwoOverlappingFilesSameRow(t *testing.T) {
	// Newer file (order=0): row1/q1@200, row1/q2@200
	cellsA := []hfile.Cell{
		cell("row1", fam, "q1", 200, hfile.CellTypePut, "a-q1-200"),
		cell("row1", fam, "q2", 200, hfile.CellTypePut, "a-q2-200"),
	}
	// Older file (order=1): row1/q1@100, row1/q2@100
	cellsB := []hfile.Cell{
		cell("row1", fam, "q1", 100, hfile.CellTypePut, "b-q1-100"),
		cell("row1", fam, "q2", 100, hfile.CellTypePut, "b-q2-100"),
	}

	rdA := openHFile(t, buildHFile(t, cellsA))
	rdB := openHFile(t, buildHFile(t, cellsB))
	got := collect(t, newRSMulti(t, []*hfile.Reader{rdA, rdB}, Options{}))

	// Expected HBase order: qualifier ASC, timestamp DESC
	type spec struct {
		qual string
		ts   uint64
	}
	want := []spec{{"q1", 200}, {"q1", 100}, {"q2", 200}, {"q2", 100}}
	if len(got) != len(want) {
		t.Fatalf("got %d cells, want %d", len(got), len(want))
	}
	for i, w := range want {
		if string(got[i].Qualifier) != w.qual {
			t.Errorf("[%d] qualifier = %q, want %q", i, got[i].Qualifier, w.qual)
		}
		if got[i].Timestamp != w.ts {
			t.Errorf("[%d] ts = %d, want %d", i, got[i].Timestamp, w.ts)
		}
	}
}

func TestDeleteSuppressesPut(t *testing.T) {
	// Delete (type=8 > Put=4) comes before Put in stream (type DESC).
	cells := []hfile.Cell{
		cell("row1", fam, "q", 100, hfile.CellTypeDelete, ""),
		cell("row1", fam, "q", 100, hfile.CellTypePut, "hidden"),
	}
	rd := openHFile(t, buildHFile(t, cells))
	got := collect(t, newRS(t, rd, Options{}))
	if len(got) != 0 {
		t.Errorf("expected 0 visible cells, got %d", len(got))
	}
}

func TestDeleteColumnSuppressesAllVersions(t *testing.T) {
	// DeleteColumn at ts=200 suppresses all puts for q1 with ts ≤ 200.
	// q2 is unaffected.
	cells := []hfile.Cell{
		cell("row1", fam, "q1", 200, hfile.CellTypeDeleteColumn, ""),
		cell("row1", fam, "q1", 200, hfile.CellTypePut, "hide-200"),
		cell("row1", fam, "q1", 100, hfile.CellTypePut, "hide-100"),
		cell("row1", fam, "q2", 100, hfile.CellTypePut, "keep"),
	}
	rd := openHFile(t, buildHFile(t, cells))
	got := collect(t, newRS(t, rd, Options{}))
	if len(got) != 1 {
		t.Fatalf("got %d cells, want 1", len(got))
	}
	if string(got[0].Qualifier) != "q2" {
		t.Errorf("qualifier = %q, want q2", got[0].Qualifier)
	}
}

func TestDeleteFamilySuppressesWholeFamily(t *testing.T) {
	// DeleteFamily at ts=200 suppresses all puts in row1 with ts ≤ 200.
	// row2 is unaffected (delete tracker resets per row).
	cells := []hfile.Cell{
		cell("row1", fam, "q1", 200, hfile.CellTypeDeleteFamily, ""),
		cell("row1", fam, "q1", 200, hfile.CellTypePut, "hide1"),
		cell("row1", fam, "q1", 100, hfile.CellTypePut, "hide2"),
		cell("row1", fam, "q2", 100, hfile.CellTypePut, "hide3"),
		cell("row2", fam, "q1", 100, hfile.CellTypePut, "keep"),
	}
	rd := openHFile(t, buildHFile(t, cells))
	got := collect(t, newRS(t, rd, Options{}))
	if len(got) != 1 {
		t.Fatalf("got %d cells, want 1", len(got))
	}
	if string(got[0].Row) != "row2" {
		t.Errorf("row = %q, want row2", got[0].Row)
	}
}

func TestDeleteInOlderFileDoesNotSuppressNewerPut(t *testing.T) {
	// Newer file (order=0): Put at ts=200.
	// Older file (order=1): point-Delete at ts=100.
	// The Put at ts=200 must remain visible.
	cellsA := []hfile.Cell{
		cell("row1", fam, "q", 200, hfile.CellTypePut, "visible"),
	}
	cellsB := []hfile.Cell{
		cell("row1", fam, "q", 100, hfile.CellTypeDelete, ""),
	}

	rdA := openHFile(t, buildHFile(t, cellsA))
	rdB := openHFile(t, buildHFile(t, cellsB))
	got := collect(t, newRSMulti(t, []*hfile.Reader{rdA, rdB}, Options{}))
	if len(got) != 1 {
		t.Fatalf("got %d cells, want 1", len(got))
	}
	if got[0].Timestamp != 200 {
		t.Errorf("ts = %d, want 200", got[0].Timestamp)
	}
}

func TestMaxVersionsOne(t *testing.T) {
	// Two versions of each of two qualifiers; only the newest should be returned.
	cells := []hfile.Cell{
		cell("row1", fam, "q1", 200, hfile.CellTypePut, "q1-200"),
		cell("row1", fam, "q1", 100, hfile.CellTypePut, "q1-100"),
		cell("row1", fam, "q2", 200, hfile.CellTypePut, "q2-200"),
		cell("row1", fam, "q2", 100, hfile.CellTypePut, "q2-100"),
	}
	rd := openHFile(t, buildHFile(t, cells))
	got := collect(t, newRS(t, rd, Options{MaxVersions: 1}))
	if len(got) != 2 {
		t.Fatalf("got %d cells, want 2", len(got))
	}
	for _, c := range got {
		if c.Timestamp != 200 {
			t.Errorf("qualifier %q: ts = %d, want 200", c.Qualifier, c.Timestamp)
		}
	}
}

func TestMaxVersionsZeroUnlimited(t *testing.T) {
	cells := []hfile.Cell{
		cell("row1", fam, "q", 300, hfile.CellTypePut, "v3"),
		cell("row1", fam, "q", 200, hfile.CellTypePut, "v2"),
		cell("row1", fam, "q", 100, hfile.CellTypePut, "v1"),
	}
	rd := openHFile(t, buildHFile(t, cells))
	got := collect(t, newRS(t, rd, Options{MaxVersions: 0}))
	if len(got) != 3 {
		t.Fatalf("got %d cells, want 3", len(got))
	}
}

func TestEmptyScannerList(t *testing.T) {
	rs, err := NewRegionScanner(nil, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if rs.Next() {
		t.Error("Next() returned true for empty scanner list")
	}
	if rs.Err() != nil {
		t.Errorf("Err() = %v, want nil", rs.Err())
	}
}
