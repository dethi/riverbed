package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"os"

	"github.com/dethi/riverbed/hfile"
)

func main() {
	dump := flag.Bool("dump", false, "dump all cells")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [flags] <hfile-path>\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(1)
	}

	path := flag.Arg(0)
	f, err := os.Open(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	rd, err := hfile.Open(f, fi.Size())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
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

	if *dump {
		fmt.Println()
		printCells(rd)
	}
}

func printTrailer(t *hfile.Trailer) {
	fmt.Println("=== Trailer ===")
	fmt.Printf("  Version:                  %d.%d\n", t.MajorVersion, t.MinorVersion)
	fmt.Printf("  Entry count:              %d\n", t.EntryCount)
	fmt.Printf("  Data index count:         %d\n", t.DataIndexCount)
	fmt.Printf("  Data index levels:        %d\n", t.NumDataIndexLevels)
	fmt.Printf("  Meta index count:         %d\n", t.MetaIndexCount)
	fmt.Printf("  Compression codec:        %d\n", t.CompressionCodec)
	fmt.Printf("  Comparator:               %s\n", t.ComparatorClassName)
	fmt.Printf("  First data block offset:  %d\n", t.FirstDataBlockOffset)
	fmt.Printf("  Last data block offset:   %d\n", t.LastDataBlockOffset)
	fmt.Printf("  File info offset:         %d\n", t.FileInfoOffset)
	fmt.Printf("  Load-on-open offset:      %d\n", t.LoadOnOpenDataOffset)
	fmt.Printf("  Total uncompressed bytes: %d\n", t.TotalUncompressedBytes)
}

func printFileInfo(info map[string][]byte) {
	fmt.Println("=== File Info ===")
	for k, v := range info {
		switch k {
		case hfile.FileInfoAvgKeyLen, hfile.FileInfoAvgValueLen:
			if len(v) == 4 {
				fmt.Printf("  %s: %d\n", k, binary.BigEndian.Uint32(v))
			} else {
				fmt.Printf("  %s: %x\n", k, v)
			}
		case hfile.FileInfoMaxMemstoreTS:
			if len(v) == 8 {
				fmt.Printf("  %s: %d\n", k, binary.BigEndian.Uint64(v))
			} else {
				fmt.Printf("  %s: %x\n", k, v)
			}
		case hfile.FileInfoDataBlockEncoding:
			fmt.Printf("  %s: %s\n", k, string(v))
		default:
			if isPrintable(v) {
				fmt.Printf("  %s: %s\n", k, string(v))
			} else {
				fmt.Printf("  %s: %x\n", k, v)
			}
		}
	}
}

func printDataIndex(idx *hfile.BlockIndex, t *hfile.Trailer) {
	fmt.Println("=== Data Index ===")
	fmt.Printf("  Levels:  %d\n", t.NumDataIndexLevels)
	fmt.Printf("  Entries: %d\n", len(idx.Entries))
	if len(idx.Entries) > 0 {
		fmt.Printf("  First block offset: %d\n", idx.Entries[0].BlockOffset)
		fmt.Printf("  Last block offset:  %d\n", idx.Entries[len(idx.Entries)-1].BlockOffset)
	}
}

func printBloomFilter(bf *hfile.BloomFilter) {
	fmt.Println("=== Bloom Filter ===")
	fmt.Printf("  Total byte size: %d\n", bf.TotalByteSize)
	fmt.Printf("  Hash count:      %d\n", bf.HashCount)
	fmt.Printf("  Hash type:       %d\n", bf.HashType)
	fmt.Printf("  Total key count: %d\n", bf.TotalKeyCount)
	fmt.Printf("  Total max keys:  %d\n", bf.TotalMaxKeys)
	fmt.Printf("  Num chunks:      %d\n", bf.NumChunks)
	if bf.Comparator != "" {
		fmt.Printf("  Comparator:      %s\n", bf.Comparator)
	}
}

func printCells(rd *hfile.Reader) {
	fmt.Println("=== Cells ===")
	scanner := rd.Scanner()
	count := 0
	for scanner.Next() {
		c := scanner.Cell()
		fmt.Printf("  %s/%s:%s/%d/%s = %s\n",
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
