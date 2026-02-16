package hfile

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
)

var (
	mvnAvailable bool
	mvnOnce      sync.Once

	serverOnce    sync.Once
	serverInst    *hfileServer
	serverInitErr error
)

func checkMvn() {
	genDir := filepath.Join("..", "testdata", "gen")
	cmd := exec.Command("mvn", "-q", "compile")
	cmd.Dir = genDir
	if err := cmd.Run(); err != nil {
		return
	}
	mvnAvailable = true
}

// hfileServer wraps the Java GenerateHFileServer process.
type hfileServer struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *bufio.Scanner
	mu     sync.Mutex
}

func getServer() (*hfileServer, error) {
	serverOnce.Do(func() {
		genDir := filepath.Join("..", "testdata", "gen")
		cmd := exec.Command(
			"mvn", "-q", "exec:java",
			"-Dexec.mainClass=GenerateHFileServer",
		)
		cmd.Dir = genDir
		// Discard Java stderr (log4j warnings, etc.) to avoid confusing fuzz workers.
		cmd.Stderr = nil

		stdin, err := cmd.StdinPipe()
		if err != nil {
			serverInitErr = fmt.Errorf("stdin pipe: %w", err)
			return
		}
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			serverInitErr = fmt.Errorf("stdout pipe: %w", err)
			return
		}

		if err := cmd.Start(); err != nil {
			serverInitErr = fmt.Errorf("start server: %w", err)
			return
		}

		serverInst = &hfileServer{
			cmd:    cmd,
			stdin:  stdin,
			stdout: bufio.NewScanner(stdout),
		}
	})
	return serverInst, serverInitErr
}

func (s *hfileServer) generate(cfg hfileConfig) error {
	return s.generateRaw(cfg)
}

func (s *hfileServer) generateRaw(v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	data = append(data, '\n')

	if _, err := s.stdin.Write(data); err != nil {
		return fmt.Errorf("write to server: %w", err)
	}

	if !s.stdout.Scan() {
		if err := s.stdout.Err(); err != nil {
			return fmt.Errorf("read from server: %w", err)
		}
		return fmt.Errorf("server closed stdout unexpectedly")
	}

	return nil
}

type hfileConfig struct {
	OutputPath        string   `json:"outputPath"`
	Compression       string   `json:"compression"`
	DataBlockEncoding string   `json:"dataBlockEncoding"`
	IncludeTags       bool     `json:"includeTags"`
	BlockSize         int      `json:"blockSize"`
	CellCount         int      `json:"cellCount"`
	Families          []string `json:"families"`
	Qualifiers        []string `json:"qualifiers"`
	ValueSize         int      `json:"valueSize"`
	Timestamp         int64    `json:"timestamp"`
}

// decodeParams decodes fuzz bytes into hfileConfig parameters.
func decodeParams(data []byte) hfileConfig {
	for len(data) < 9 {
		data = append(data, 0)
	}

	// compression: NONE or GZ
	compressions := []string{"NONE", "GZ", "SNAPPY", "ZSTD"}
	compression := compressions[data[0]%uint8(len(compressions))]

	includeTags := data[0]/uint8(len(compressions))%2 == 1

	// blockSize: 64 to 65536
	blockSize := 64 + int(binary.BigEndian.Uint16(data[1:3]))%(65536-64+1)

	// cellCount: 1 to 1000
	cellCount := 1 + int(binary.BigEndian.Uint16(data[3:5]))%1000

	// valueSize: 0 to 1000
	valueSize := int(binary.BigEndian.Uint16(data[5:7])) % 1001

	// family: pick one from a fixed pool. In HBase each StoreFile/HFile
	// belongs to a single column family, so we always use exactly one.
	familyPool := []string{"a", "bb", "ccc"}
	family := familyPool[data[0]/uint8(len(compressions))/2%uint8(len(familyPool))]
	families := []string{family}

	// qualifiers: 1-3, from a fixed pool
	qualPool := []string{"x", "yy", "zzz"}
	numQualifiers := 1 + int(data[7])%3
	qualifiers := qualPool[:numQualifiers]

	// dataBlockEncoding: NONE or FAST_DIFF
	encodings := []string{"NONE", "FAST_DIFF"}
	encoding := encodings[data[8]%uint8(len(encodings))]

	return hfileConfig{
		Compression:       compression,
		DataBlockEncoding: encoding,
		IncludeTags:       includeTags,
		BlockSize:         blockSize,
		CellCount:         cellCount,
		Families:          families,
		Qualifiers:        qualifiers,
		ValueSize:         valueSize,
		Timestamp:         1700000000000,
	}
}

// generateExpectedValue produces the same value bytes as the Java side.
func generateExpectedValue(index, valueSize int) string {
	if valueSize == 0 {
		return ""
	}
	s := fmt.Sprintf("%0*d", valueSize, index)
	if len(s) > valueSize {
		s = s[len(s)-valueSize:]
	}
	return s
}

func FuzzReadHFile(f *testing.F) {
	mvnOnce.Do(checkMvn)
	if !mvnAvailable {
		f.Skip("mvn not available or compilation failed")
	}

	// Add seed corpus entries covering interesting parameter combos.
	f.Add([]byte{0x00, 0xFF, 0x00, 0x00, 0x0A, 0x00, 0x06, 0x00, 0x00}) // NONE, no tags, normal block, 10 cells
	f.Add([]byte{0x01, 0x00, 0x40, 0x00, 0x32, 0x00, 0x0A, 0x00, 0x00}) // NONE, tags, small blocks, 50 cells
	f.Add([]byte{0x04, 0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x02, 0x00}) // NONE, no tags, tiny block, 100 cells
	f.Add([]byte{0x03, 0xFF, 0xFF, 0x00, 0x01, 0x00, 0x64, 0x02, 0x00}) // NONE, tags, large block, 1 cell, multi-qualifier
	f.Add([]byte{0x01, 0x00, 0x40, 0x00, 0x14, 0x00, 0x0A, 0x00, 0x00}) // NONE, GZ compression, 20 cells
	f.Add([]byte{0x02, 0x00, 0x40, 0x00, 0x14, 0x00, 0x0A, 0x00, 0x00}) // NONE, SNAPPY compression, 20 cells
	f.Add([]byte{0x03, 0x00, 0x40, 0x00, 0x14, 0x00, 0x0A, 0x00, 0x00}) // NONE, ZSTD compression, 20 cells
	f.Add([]byte{0x00, 0xFF, 0x00, 0x00, 0x0A, 0x00, 0x06, 0x00, 0x01}) // FAST_DIFF, no tags, 10 cells
	f.Add([]byte{0x01, 0x00, 0x40, 0x00, 0x32, 0x00, 0x0A, 0x00, 0x01}) // FAST_DIFF, tags, 50 cells
	f.Add([]byte{0x04, 0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x02, 0x01}) // FAST_DIFF, tiny block, 100 cells
	f.Add([]byte{0x03, 0xFF, 0xFF, 0x00, 0x01, 0x00, 0x64, 0x02, 0x01}) // FAST_DIFF, tags, 1 cell
	f.Add([]byte{0x00, 0x00, 0x40, 0x00, 0x14, 0x00, 0x0A, 0x00, 0x01}) // FAST_DIFF, NONE compression, 20 cells

	f.Fuzz(func(t *testing.T, data []byte) {
		srv, err := getServer()
		if err != nil {
			t.Fatalf("start server: %v", err)
		}

		cfg := decodeParams(data)
		cfg.OutputPath = filepath.Join(t.TempDir(), "test.hfile")

		if err := srv.generate(cfg); err != nil {
			t.Fatalf("generate hfile: %v", err)
		}

		verifyHFile(t, cfg)
	})
}

func verifyHFile(t *testing.T, cfg hfileConfig) {
	t.Helper()

	file, err := os.Open(cfg.OutputPath)
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

	// Sort families and qualifiers the same way Java does.
	families := make([]string, len(cfg.Families))
	copy(families, cfg.Families)
	sort.Strings(families)

	qualifiers := make([]string, len(cfg.Qualifiers))
	copy(qualifiers, cfg.Qualifiers)
	sort.Strings(qualifiers)

	totalCells := cfg.CellCount * len(families) * len(qualifiers)

	// Property 1: EntryCount matches.
	if got := int(rd.Trailer().EntryCount); got != totalCells {
		t.Errorf("EntryCount = %d, want %d", got, totalCells)
	}

	// Scan all cells.
	scanner := rd.Scanner()
	count := 0
	var prevRow, prevFamily, prevQualifier string
	var prevTimestamp uint64

	for scanner.Next() {
		c := scanner.Cell()

		// Determine expected cell based on position.
		cellIdx := count
		qualIdx := cellIdx % len(qualifiers)
		cellIdx /= len(qualifiers)
		famIdx := cellIdx % len(families)
		rowIdx := cellIdx / len(families)

		expectedRow := fmt.Sprintf("row-%05d", rowIdx)
		expectedFamily := families[famIdx]
		expectedQualifier := qualifiers[qualIdx]
		expectedValue := generateExpectedValue(rowIdx, cfg.ValueSize)

		// Property 5: Cell content matches deterministic generation.
		if string(c.Row) != expectedRow {
			t.Errorf("cell %d: row = %q, want %q", count, c.Row, expectedRow)
		}
		if string(c.Family) != expectedFamily {
			t.Errorf("cell %d: family = %q, want %q", count, c.Family, expectedFamily)
		}
		if string(c.Qualifier) != expectedQualifier {
			t.Errorf("cell %d: qualifier = %q, want %q", count, c.Qualifier, expectedQualifier)
		}
		if c.Timestamp != uint64(cfg.Timestamp) {
			t.Errorf("cell %d: timestamp = %d, want %d", count, c.Timestamp, cfg.Timestamp)
		}
		if string(c.Value) != expectedValue {
			t.Errorf("cell %d: value = %q, want %q", count, c.Value, expectedValue)
		}
		if c.Type != CellTypePut {
			t.Errorf("cell %d: type = %v, want Put", count, c.Type)
		}

		// Property 4: HBase sort order (row, family, qualifier, timestamp desc).
		curRow := string(c.Row)
		curFamily := string(c.Family)
		curQualifier := string(c.Qualifier)
		if count > 0 {
			cmp := strings.Compare(curRow, prevRow)
			if cmp < 0 {
				t.Errorf("cell %d: row %q < previous row %q", count, curRow, prevRow)
			} else if cmp == 0 {
				cmp = strings.Compare(curFamily, prevFamily)
				if cmp < 0 {
					t.Errorf("cell %d: family %q < previous family %q", count, curFamily, prevFamily)
				} else if cmp == 0 {
					cmp = strings.Compare(curQualifier, prevQualifier)
					if cmp < 0 {
						t.Errorf("cell %d: qualifier %q < previous qualifier %q", count, curQualifier, prevQualifier)
					} else if cmp == 0 {
						if c.Timestamp > prevTimestamp {
							t.Errorf("cell %d: timestamp %d > previous %d (should be descending)", count, c.Timestamp, prevTimestamp)
						}
					}
				}
			}
		}
		prevRow = curRow
		prevFamily = curFamily
		prevQualifier = curQualifier
		prevTimestamp = c.Timestamp

		count++
	}

	// Property 6: No scanner error.
	if err := scanner.Err(); err != nil {
		t.Fatalf("scanner error: %v", err)
	}

	// Property 3: Scanner yields exactly totalCells.
	if count != totalCells {
		t.Errorf("scanned %d cells, want %d", count, totalCells)
	}
}
