package main

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/dethi/riverbed/hfile"
	"github.com/dethi/riverbed/scanner"
)

// Print functions take an indent string for formatting values.
// The caller is responsible for printing any section header.

func printTrailer(t *hfile.Trailer, indent string) {
	fmt.Printf("%sVersion:                  %d.%d\n", indent, t.MajorVersion, t.MinorVersion)
	fmt.Printf("%sEntry count:              %d\n", indent, t.EntryCount)
	fmt.Printf("%sData index count:         %d\n", indent, t.DataIndexCount)
	fmt.Printf("%sData index levels:        %d\n", indent, t.NumDataIndexLevels)
	fmt.Printf("%sMeta index count:         %d\n", indent, t.MetaIndexCount)
	fmt.Printf("%sCompression codec:        %d\n", indent, t.CompressionCodec)
	fmt.Printf("%sComparator:               %s\n", indent, t.ComparatorClassName)
	fmt.Printf("%sFirst data block offset:  %d\n", indent, t.FirstDataBlockOffset)
	fmt.Printf("%sLast data block offset:   %d\n", indent, t.LastDataBlockOffset)
	fmt.Printf("%sFile info offset:         %d\n", indent, t.FileInfoOffset)
	fmt.Printf("%sLoad-on-open offset:      %d\n", indent, t.LoadOnOpenDataOffset)
	fmt.Printf("%sTotal uncompressed bytes: %d\n", indent, t.TotalUncompressedBytes)
}

func printFileInfo(info map[string][]byte, indent string) {
	for k, v := range info {
		switch k {
		case hfile.FileInfoAvgKeyLen, hfile.FileInfoAvgValueLen:
			if len(v) == 4 {
				fmt.Printf("%s%s: %d\n", indent, k, binary.BigEndian.Uint32(v))
			} else {
				fmt.Printf("%s%s: %x\n", indent, k, v)
			}
		case hfile.FileInfoMaxMemstoreTS:
			if len(v) == 8 {
				fmt.Printf("%s%s: %d\n", indent, k, binary.BigEndian.Uint64(v))
			} else {
				fmt.Printf("%s%s: %x\n", indent, k, v)
			}
		case hfile.FileInfoDataBlockEncoding:
			fmt.Printf("%s%s: %s\n", indent, k, string(v))
		default:
			if isPrintable(v) {
				fmt.Printf("%s%s: %s\n", indent, k, string(v))
			} else {
				fmt.Printf("%s%s: %x\n", indent, k, v)
			}
		}
	}
}

func printDataIndex(idx *hfile.BlockIndex, t *hfile.Trailer, indent string) {
	fmt.Printf("%sLevels:  %d\n", indent, t.NumDataIndexLevels)
	fmt.Printf("%sEntries: %d\n", indent, len(idx.Entries))
	if len(idx.Entries) > 0 {
		fmt.Printf("%sFirst block offset: %d\n", indent, idx.Entries[0].BlockOffset)
		fmt.Printf("%sLast block offset:  %d\n", indent, idx.Entries[len(idx.Entries)-1].BlockOffset)
	}
}

func printBloomFilter(bf *hfile.BloomFilter, indent string) {
	fmt.Printf("%sTotal byte size: %d\n", indent, bf.TotalByteSize)
	fmt.Printf("%sHash count:      %d\n", indent, bf.HashCount)
	fmt.Printf("%sHash type:       %d\n", indent, bf.HashType)
	fmt.Printf("%sTotal key count: %d\n", indent, bf.TotalKeyCount)
	fmt.Printf("%sTotal max keys:  %d\n", indent, bf.TotalMaxKeys)
	fmt.Printf("%sNum chunks:      %d\n", indent, bf.NumChunks)
	if bf.Comparator != "" {
		fmt.Printf("%sComparator:      %s\n", indent, bf.Comparator)
	}
}

func printRegionCells(rs *scanner.RegionScanner, indent string) {
	count := 0
	for rs.Next() {
		c := rs.Cell()
		fmt.Printf("%s%s/%s:%s/%d/%s = %s\n",
			indent,
			formatBytes(c.Row),
			formatBytes(c.Family),
			formatBytes(c.Qualifier),
			c.Timestamp,
			c.Type,
			formatBytes(c.Value),
		)
		count++
	}
	if err := rs.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Scanner error: %v\n", err)
	}
	fmt.Printf("%sTotal: %d cells\n", indent, count)
}

func printCells(rd *hfile.Reader, indent string) {
	scanner := rd.Scanner()
	count := 0
	for scanner.Next() {
		c := scanner.Cell()
		fmt.Printf("%s%s/%s:%s/%d/%s = %s\n",
			indent,
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
	fmt.Printf("%sTotal: %d cells\n", indent, count)
}

func printHFileContent(rd *hfile.Reader, indent string, dump bool) {
	vi := indent + "  "
	fmt.Printf("%sTrailer:\n", indent)
	printTrailer(rd.Trailer(), vi)
	fmt.Println()
	fmt.Printf("%sFile Info:\n", indent)
	printFileInfo(rd.FileInfo(), vi)
	fmt.Println()
	fmt.Printf("%sData Index:\n", indent)
	printDataIndex(rd.DataIndex(), rd.Trailer(), vi)
	if bf := rd.BloomFilter(); bf != nil {
		fmt.Println()
		fmt.Printf("%sBloom Filter:\n", indent)
		printBloomFilter(bf, vi)
	}
	if dump {
		fmt.Println()
		fmt.Printf("%sCells:\n", indent)
		printCells(rd, vi)
	}
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
