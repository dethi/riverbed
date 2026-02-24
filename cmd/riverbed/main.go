package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/dethi/riverbed/storage"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "riverbed",
	Short: "Tools for working with HBase HFiles and snapshots",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		verbose, _ := cmd.Flags().GetBool("verbose")
		if verbose {
			h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
				ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
					if a.Key == slog.TimeKey {
						return slog.Attr{} // omit timestamp for concise CLI output
					}
					return a
				},
			})
			storage.SetLogger(slog.New(h))
		}
		return nil
	},
}

func init() {
	rootCmd.PersistentFlags().Bool("verbose", false, "log GCS requests and latency to stderr")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
