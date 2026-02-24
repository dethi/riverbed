package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"os"
	"slices"
	"strconv"
	"time"

	"github.com/dethi/riverbed/hfile"
	"github.com/dethi/riverbed/scanner"
	"github.com/dethi/riverbed/snapshot"
	"github.com/dethi/riverbed/storage"
	pb "github.com/dethi/riverbed/snapshot/proto"
	"github.com/spf13/cobra"
)

var snapshotCmd = &cobra.Command{
	Use:   "snapshot <snapshot-dir> <data-dir>",
	Short: "Inspect an HBase snapshot and its HFiles",
	Args:  cobra.ExactArgs(2),
	RunE:  runSnapshot,
}

func init() {
	snapshotCmd.Flags().Bool("dump", false, "dump merged cells per region/family")
	snapshotCmd.Flags().Int("max-versions", 0, "max cell versions to return per column (0 = unlimited)")
	rootCmd.AddCommand(snapshotCmd)
}

func runSnapshot(cmd *cobra.Command, args []string) error {
	dump, _ := cmd.Flags().GetBool("dump")
	maxVersions, _ := cmd.Flags().GetInt("max-versions")
	snapshotDir := args[0]
	dataDir := args[1]

	fsys, err := storage.OpenDir(cmd.Context(), snapshotDir)
	if err != nil {
		return err
	}

	m, err := snapshot.ReadManifest(fsys)
	if err != nil {
		return err
	}

	printDescription(m.Description)

	if m.TableSchema != nil {
		fmt.Println()
		printTableSchema(m.TableSchema)
	}

	// Sort regions by start key.
	regions := m.Regions
	slices.SortFunc(regions, func(a, b *pb.SnapshotRegionManifest) int {
		return bytes.Compare(
			a.GetRegionInfo().GetStartKey(),
			b.GetRegionInfo().GetStartKey(),
		)
	})

	for _, region := range regions {
		ri := region.GetRegionInfo()
		fmt.Println()
		printRegionInfo(ri)

		encodedName := encodeRegionName(ri)
		for _, ff := range region.GetFamilyFiles() {
			family := string(ff.GetFamilyName())

			var (
				readers []*hfile.Reader
				closers []func()
			)

			for _, sf := range ff.GetStoreFiles() {
				// Split references point into a parent region's HFile; skip them.
				if sf.GetReference() != nil {
					fmt.Fprintf(os.Stderr, "  Note: skipping split reference %s/%s\n", family, sf.GetName())
					continue
				}

				path := storage.JoinPath(dataDir, encodedName, family, sf.GetName())
				fmt.Println()
				fmt.Printf("--- HFile: %s/%s/%s ---\n", encodedName, family, sf.GetName())

				close, rd, err := openHFile(cmd.Context(), path)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error reading %s: %v\n", path, err)
					continue
				}
				closers = append(closers, close)
				readers = append(readers, rd)
				printHFileContent(rd, "  ", false)
			}

			if dump && len(readers) > 0 {
				fmt.Println()
				fmt.Printf("--- Merged Cells: %s/%s ---\n", encodedName, family)
				scanners := make([]*hfile.Scanner, len(readers))
				for i, rd := range readers {
					scanners[i] = rd.Scanner()
				}
				rs, err := scanner.NewRegionScanner(scanners, scanner.Options{MaxVersions: maxVersions})
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error creating scanner: %v\n", err)
				} else {
					printRegionCells(rs, "  ")
				}
			}

			for _, close := range closers {
				close()
			}
		}
	}

	return nil
}

func printDescription(desc *pb.SnapshotDescription) {
	fmt.Println("=== Snapshot ===")
	fmt.Printf("  Name:          %s\n", desc.GetName())
	fmt.Printf("  Table:         %s\n", desc.GetTable())
	fmt.Printf("  Type:          %s\n", desc.GetType())
	fmt.Printf("  Version:       %d\n", desc.GetVersion())
	if t := desc.GetCreationTime(); t != 0 {
		fmt.Printf("  Creation time: %s\n", time.UnixMilli(t).UTC().Format(time.RFC3339))
	}
	if owner := desc.GetOwner(); owner != "" {
		fmt.Printf("  Owner:         %s\n", owner)
	}
	if ttl := desc.GetTtl(); ttl != 0 {
		fmt.Printf("  TTL:           %d\n", ttl)
	}
}

func printTableSchema(ts *pb.TableSchema) {
	fmt.Println("=== Table Schema ===")
	if tn := ts.GetTableName(); tn != nil {
		ns := string(tn.GetNamespace())
		q := string(tn.GetQualifier())
		if ns == "default" {
			fmt.Printf("  Name: %s\n", q)
		} else {
			fmt.Printf("  Name: %s:%s\n", ns, q)
		}
	}
	for _, cf := range ts.GetColumnFamilies() {
		fmt.Printf("  Column family: %s\n", string(cf.GetName()))
	}
}

func printRegionInfo(ri *pb.RegionInfo) {
	fmt.Println("=== Region ===")
	fmt.Printf("  Region ID:  %d\n", ri.GetRegionId())
	fmt.Printf("  Start key:  %s\n", formatBytes(ri.GetStartKey()))
	fmt.Printf("  End key:    %s\n", formatBytes(ri.GetEndKey()))
	if ri.GetReplicaId() != 0 {
		fmt.Printf("  Replica ID: %d\n", ri.GetReplicaId())
	}
}

func encodeRegionName(ri *pb.RegionInfo) string {
	tn := ri.GetTableName()
	ns := tn.GetNamespace()
	qualifier := tn.GetQualifier()

	var tableName []byte
	if string(ns) == "default" {
		tableName = qualifier
	} else {
		tableName = make([]byte, len(ns)+1+len(qualifier))
		copy(tableName, ns)
		tableName[len(ns)] = ':'
		copy(tableName[len(ns)+1:], qualifier)
	}

	regionID := strconv.FormatUint(ri.GetRegionId(), 10)

	startKey := ri.GetStartKey()
	buf := make([]byte, 0, len(tableName)+1+len(startKey)+1+len(regionID))
	buf = append(buf, tableName...)
	buf = append(buf, ',')
	buf = append(buf, startKey...)
	buf = append(buf, ',')
	buf = append(buf, regionID...)

	hash := md5.Sum(buf)
	return fmt.Sprintf("%x", hash)
}

// openHFile opens an HFile at path (local or gs://) and returns a close
// function and reader. The caller is responsible for calling close.
func openHFile(ctx context.Context, path string) (func(), *hfile.Reader, error) {
	r, size, close, err := storage.OpenFile(ctx, path)
	if err != nil {
		return nil, nil, err
	}
	rd, err := hfile.Open(r, size)
	if err != nil {
		close()
		return nil, nil, err
	}
	return close, rd, nil
}
