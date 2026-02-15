package hfile

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

const (
	checksumNull   = 0
	checksumCRC32  = 1
	checksumCRC32C = 2
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// verifyChecksums verifies the checksums of a block.
// header is the raw 33-byte block header, data is the decompressed data (without header),
// and checksumBytes contains the appended checksums.
func verifyChecksums(checksumType byte, bytesPerChecksum int, header, data, checksumBytes []byte) error {
	if bytesPerChecksum <= 0 {
		return fmt.Errorf("hfile: invalid bytesPerChecksum %d", bytesPerChecksum)
	}

	// Checksums are computed over chunks of (header + data) combined.
	combined := make([]byte, len(header)+len(data))
	copy(combined, header)
	copy(combined[len(header):], data)

	numChunks := (len(combined) + bytesPerChecksum - 1) / bytesPerChecksum
	if len(checksumBytes) < numChunks*4 {
		return fmt.Errorf("hfile: checksum data too short: need %d bytes, have %d", numChunks*4, len(checksumBytes))
	}

	for i := range numChunks {
		start := i * bytesPerChecksum
		end := min(start+bytesPerChecksum, len(combined))
		chunk := combined[start:end]

		expected := binary.BigEndian.Uint32(checksumBytes[i*4 : i*4+4])
		var got uint32

		switch checksumType {
		case checksumCRC32:
			got = crc32.ChecksumIEEE(chunk)
		case checksumCRC32C:
			got = crc32.Checksum(chunk, crc32cTable)
		default:
			return fmt.Errorf("hfile: unsupported checksum type %d", checksumType)
		}

		if got != expected {
			return fmt.Errorf("hfile: checksum mismatch in chunk %d: got %08x, want %08x", i, got, expected)
		}
	}
	return nil
}
