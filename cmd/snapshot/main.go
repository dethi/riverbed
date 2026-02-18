package main

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"github.com/dethi/riverbed/hfile"
	"github.com/dethi/riverbed/snapshot"
	pb "github.com/dethi/riverbed/snapshot/proto"
)

func main() {
	dump := flag.Bool("dump", false, "dump all cells")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [flags] <snapshot-dir> <data-dir>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nArguments:\n")
		fmt.Fprintf(os.Stderr, "  snapshot-dir  path to the snapshot directory\n")
		fmt.Fprintf(os.Stderr, "  data-dir      path to the table data directory containing region HFiles\n")
		fmt.Fprintf(os.Stderr, "\nFlags:\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	if flag.NArg() != 2 {
		flag.Usage()
		os.Exit(1)
	}

	snapshotDir := flag.Arg(0)
	dataDir := flag.Arg(1)

	m, err := snapshot.ReadManifest(os.DirFS(snapshotDir))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
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
				if err := printHFile(path, *dump); err != nil {
					fmt.Fprintf(os.Stderr, "Error reading %s: %v\n", path, err)
				}
			}
		}
	}
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

// encodeRegionName computes the MD5 hex-encoded region name from a RegionInfo,
// matching HBase's RegionInfo.createRegionName + encodeRegionName.
// Region name format: <tableName>,<startKey>,<regionId>
func encodeRegionName(ri *pb.RegionInfo) string {
	tn := ri.GetTableName()
	ns := tn.GetNamespace()
	qualifier := tn.GetQualifier()

	// TableName.getName(): default namespace uses just qualifier,
	// others use "namespace:qualifier".
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

	// Build: <tableName>,<startKey>,<regionId>
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

func printHFile(path string, dump bool) error {
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

	printTrailer(rd.Trailer())
	fmt.Println()
	printFileInfo(rd.FileInfo())
	fmt.Println()
	printDataIndex(rd.DataIndex(), rd.Trailer())
	if bf := rd.BloomFilter(); bf != nil {
		fmt.Println()
		printBloomFilter(bf)
	}

	if dump {
		fmt.Println()
		printCells(rd)
	}

	return nil
}

func printTrailer(t *hfile.Trailer) {
	fmt.Println("  Trailer:")
	fmt.Printf("    Version:                  %d.%d\n", t.MajorVersion, t.MinorVersion)
	fmt.Printf("    Entry count:              %d\n", t.EntryCount)
	fmt.Printf("    Data index count:         %d\n", t.DataIndexCount)
	fmt.Printf("    Data index levels:        %d\n", t.NumDataIndexLevels)
	fmt.Printf("    Meta index count:         %d\n", t.MetaIndexCount)
	fmt.Printf("    Compression codec:        %d\n", t.CompressionCodec)
	fmt.Printf("    Comparator:               %s\n", t.ComparatorClassName)
	fmt.Printf("    First data block offset:  %d\n", t.FirstDataBlockOffset)
	fmt.Printf("    Last data block offset:   %d\n", t.LastDataBlockOffset)
	fmt.Printf("    File info offset:         %d\n", t.FileInfoOffset)
	fmt.Printf("    Load-on-open offset:      %d\n", t.LoadOnOpenDataOffset)
	fmt.Printf("    Total uncompressed bytes: %d\n", t.TotalUncompressedBytes)
}

func printFileInfo(info map[string][]byte) {
	fmt.Println("  File Info:")
	for k, v := range info {
		switch k {
		case hfile.FileInfoAvgKeyLen, hfile.FileInfoAvgValueLen:
			if len(v) == 4 {
				fmt.Printf("    %s: %d\n", k, binary.BigEndian.Uint32(v))
			} else {
				fmt.Printf("    %s: %x\n", k, v)
			}
		case hfile.FileInfoMaxMemstoreTS:
			if len(v) == 8 {
				fmt.Printf("    %s: %d\n", k, binary.BigEndian.Uint64(v))
			} else {
				fmt.Printf("    %s: %x\n", k, v)
			}
		case hfile.FileInfoDataBlockEncoding:
			fmt.Printf("    %s: %s\n", k, string(v))
		default:
			if isPrintable(v) {
				fmt.Printf("    %s: %s\n", k, string(v))
			} else {
				fmt.Printf("    %s: %x\n", k, v)
			}
		}
	}
}

func printDataIndex(idx *hfile.BlockIndex, t *hfile.Trailer) {
	fmt.Println("  Data Index:")
	fmt.Printf("    Levels:  %d\n", t.NumDataIndexLevels)
	fmt.Printf("    Entries: %d\n", len(idx.Entries))
	if len(idx.Entries) > 0 {
		fmt.Printf("    First block offset: %d\n", idx.Entries[0].BlockOffset)
		fmt.Printf("    Last block offset:  %d\n", idx.Entries[len(idx.Entries)-1].BlockOffset)
	}
}

func printBloomFilter(bf *hfile.BloomFilter) {
	fmt.Println("  Bloom Filter:")
	fmt.Printf("    Total byte size: %d\n", bf.TotalByteSize)
	fmt.Printf("    Hash count:      %d\n", bf.HashCount)
	fmt.Printf("    Hash type:       %d\n", bf.HashType)
	fmt.Printf("    Total key count: %d\n", bf.TotalKeyCount)
	fmt.Printf("    Total max keys:  %d\n", bf.TotalMaxKeys)
	fmt.Printf("    Num chunks:      %d\n", bf.NumChunks)
	if bf.Comparator != "" {
		fmt.Printf("    Comparator:      %s\n", bf.Comparator)
	}
}

func printCells(rd *hfile.Reader) {
	fmt.Println("  Cells:")
	scanner := rd.Scanner()
	count := 0
	for scanner.Next() {
		c := scanner.Cell()
		fmt.Printf("    %s/%s:%s/%d/%s = %s\n",
			formatBytes(c.Row),
			formatBytes(c.Family),
			formatBytes(c.Qualifier),
			c.Timestamp,
			c.Type,
			formatBytes(c.Value),
		)
		count++
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Scanner error: %v\n", err)
	}
	fmt.Printf("  Total: %d cells\n", count)
}

func formatBytes(b []byte) string {
	if len(b) == 0 {
		return "<empty>"
	}
	if isPrintable(b) {
		return string(b)
	}
	return fmt.Sprintf("%x", b)
}

func isPrintable(b []byte) bool {
	for _, c := range b {
		if c < 0x20 || c > 0x7e {
			return false
		}
	}
	return true
}
