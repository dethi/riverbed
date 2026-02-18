package main

import (
	"fmt"
	"os"

	"github.com/dethi/riverbed/hfile"
	"github.com/spf13/cobra"
)

var hfileCmd = &cobra.Command{
	Use:   "hfile <hfile-path>",
	Short: "Inspect an HFile",
	Args:  cobra.ExactArgs(1),
	RunE:  runHFile,
}

func init() {
	hfileCmd.Flags().Bool("dump", false, "dump all cells")
	rootCmd.AddCommand(hfileCmd)
}

func runHFile(cmd *cobra.Command, args []string) error {
	dump, _ := cmd.Flags().GetBool("dump")
	path := args[0]

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

	fmt.Println("=== Trailer ===")
	printTrailer(rd.Trailer(), "  ")
	fmt.Println()
	fmt.Println("=== File Info ===")
	printFileInfo(rd.FileInfo(), "  ")
	fmt.Println()
	fmt.Println("=== Data Index ===")
	printDataIndex(rd.DataIndex(), rd.Trailer(), "  ")
	if bf := rd.BloomFilter(); bf != nil {
		fmt.Println()
		fmt.Println("=== Bloom Filter ===")
		printBloomFilter(bf, "  ")
	}

	if dump {
		fmt.Println()
		fmt.Println("=== Cells ===")
		printCells(rd, "  ")
	}

	return nil
}
