package main

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"github.com/dethi/riverbed/hfile"
	pb "github.com/dethi/riverbed/snapshot/proto"

	"github.com/dethi/riverbed/snapshot"
	"github.com/spf13/cobra"
)

var snapshotCmd = &cobra.Command{
	Use:   "snapshot <snapshot-dir> <data-dir>",
	Short: "Inspect an HBase snapshot and its HFiles",
	Args:  cobra.ExactArgs(2),
	RunE:  runSnapshot,
}

func init() {
	snapshotCmd.Flags().Bool("dump", false, "dump all cells")
	rootCmd.AddCommand(snapshotCmd)
}

func runSnapshot(cmd *cobra.Command, args []string) error {
	dump, _ := cmd.Flags().GetBool("dump")
	snapshotDir := args[0]
	dataDir := args[1]

	m, err := snapshot.ReadManifest(os.DirFS(snapshotDir))
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
			for _, sf := range ff.GetStoreFiles() {
				path := filepath.Join(dataDir, encodedName, family, sf.GetName())
				fmt.Println()
				fmt.Printf("--- HFile: %s/%s/%s ---\n", encodedName, family, sf.GetName())
				if err := openAndPrintHFile(path, dump); err != nil {
					fmt.Fprintf(os.Stderr, "Error reading %s: %v\n", path, err)
				}
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

func openAndPrintHFile(path string, dump bool) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return err
	}

	rd, err := hfile.Open(f, fi.Size())
	if err != nil {
		return err
	}

	printHFileContent(rd, "  ", dump)
	return nil
}
