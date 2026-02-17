package hfile

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"

	"pgregory.net/rapid"
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

func FuzzReadHFile(f *testing.F) {
	mvnOnce.Do(checkMvn)
	if !mvnAvailable {
		f.Skip("mvn not available or compilation failed")
	}

	f.Fuzz(rapid.MakeFuzz(func(t *rapid.T) {
		srv, err := getServer()
		if err != nil {
			t.Fatalf("start server: %v", err)
		}

		r := genRecipe(t)

		if estimateRecipeSize(r) > maxRecipeBytes {
			t.Skip("recipe too large")
		}

		expectedCells := expandRecipe(r)
		if len(expectedCells) == 0 {
			t.Skip("no cells")
		}

		tmpDir, err := os.MkdirTemp("", "fuzz-hfile-*")
		if err != nil {
			t.Fatalf("create temp dir: %v", err)
		}
		t.Cleanup(func() { os.RemoveAll(tmpDir) })
		t.Logf("%s: tmp dir: %s", t.Name(), tmpDir)

		r.OutputPath = filepath.Join(tmpDir, fmt.Sprintf("test-%d.hfile", rapid.Int64().Draw(t, "fileid")))

		if err := srv.generateRaw(r); err != nil {
			t.Fatalf("generate hfile: %v", err)
		}

		verifyHFileProperties(t, r.OutputPath, r.BloomType, expectedCells)
	}))
}

func FuzzNoCrash(f *testing.F) {
	f.Fuzz(func(t *testing.T, b []byte) {
		if r, err := Open(bytes.NewReader(b), int64(len(b))); err == nil {
			scan := r.Scanner()
			for scan.Next() {
			}
		}
	})
}
