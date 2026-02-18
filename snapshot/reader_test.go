package snapshot

import (
	"os"
	"testing"
)

func TestReadManifest(t *testing.T) {
	dir := os.DirFS("testdata/test_snapshot")
	m, err := ReadManifest(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Verify description.
	if got := m.Description.GetName(); got != "test_snapshot" {
		t.Errorf("snapshot name = %q, want %q", got, "test_snapshot")
	}
	if got := m.Description.GetTable(); got != "test_table" {
		t.Errorf("table name = %q, want %q", got, "test_table")
	}
	if m.Description.GetCreationTime() == 0 {
		t.Error("creation_time should be non-zero")
	}

	// Verify table schema.
	if m.TableSchema == nil {
		t.Fatal("table schema is nil")
	}
	tableName := m.TableSchema.GetTableName()
	if tableName == nil {
		t.Fatal("table schema table_name is nil")
	}
	if got := string(tableName.GetQualifier()); got != "test_table" {
		t.Errorf("table schema qualifier = %q, want %q", got, "test_table")
	}

	// Verify column families exist in schema.
	cfs := m.TableSchema.GetColumnFamilies()
	if len(cfs) != 2 {
		t.Fatalf("column families count = %d, want 2", len(cfs))
	}
	cfNames := make(map[string]bool)
	for _, cf := range cfs {
		cfNames[string(cf.GetName())] = true
	}
	if !cfNames["cf1"] || !cfNames["cf2"] {
		t.Errorf("column families = %v, want cf1 and cf2", cfNames)
	}

	// Verify regions.
	if len(m.Regions) == 0 {
		t.Fatal("no regions found")
	}

	// Verify we have store files with data.
	totalFiles := 0
	for _, region := range m.Regions {
		for _, ff := range region.GetFamilyFiles() {
			totalFiles += len(ff.GetStoreFiles())
		}
	}
	if totalFiles == 0 {
		t.Error("no store files found in any region")
	}
}
