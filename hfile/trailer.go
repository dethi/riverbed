package hfile

import (
	"encoding/binary"
	"fmt"
	"io"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	pb "github.com/dethi/riverbed/hfile/proto"
)

const (
	trailerSize    = 4096 // v3 trailer is a fixed 4096 bytes
	versionSize    = 4    // 4-byte version at end of file
	majorVersionV3 = 3
)

// Trailer holds the parsed HFile trailer.
type Trailer struct {
	MajorVersion              int
	MinorVersion              int
	FileInfoOffset            uint64
	LoadOnOpenDataOffset      uint64
	UncompressedDataIndexSize uint64
	TotalUncompressedBytes    uint64
	DataIndexCount            uint32
	MetaIndexCount            uint32
	EntryCount                uint64
	NumDataIndexLevels        uint32
	FirstDataBlockOffset      uint64
	LastDataBlockOffset       uint64
	ComparatorClassName       string
	CompressionCodec          uint32
	EncryptionKey             []byte
}

// ReadTrailer reads and parses the HFile trailer from the end of the file.
func ReadTrailer(r io.ReaderAt, fileSize int64) (*Trailer, error) {
	if fileSize < trailerSize {
		return nil, fmt.Errorf("hfile: file too small (%d bytes) for trailer", fileSize)
	}

	// Read the last 4 bytes to get the version.
	var vBuf [versionSize]byte
	if _, err := r.ReadAt(vBuf[:], fileSize-versionSize); err != nil {
		return nil, fmt.Errorf("hfile: read version: %w", err)
	}
	version := binary.BigEndian.Uint32(vBuf[:])

	// Major version is in the lower 3 bytes, minor version in the upper byte.
	majorVersion := int(version & 0x00FFFFFF)
	minorVersion := int((version >> 24) & 0xFF)

	if majorVersion != majorVersionV3 {
		return nil, fmt.Errorf("hfile: unsupported major version %d (only v3 supported)", majorVersion)
	}

	// Read the full trailer.
	trailerBuf := make([]byte, trailerSize)
	if _, err := r.ReadAt(trailerBuf, fileSize-trailerSize); err != nil {
		return nil, fmt.Errorf("hfile: read trailer: %w", err)
	}

	// First 8 bytes must be the TRAILER magic.
	var magic [magicLen]byte
	copy(magic[:], trailerBuf[:magicLen])
	if magic != magicTrailer {
		return nil, fmt.Errorf("hfile: invalid trailer magic %q", magic)
	}

	// Parse the protobuf (delimited format: varint length prefix + message).
	pbData := trailerBuf[magicLen : trailerSize-versionSize]

	// Read the delimited length prefix.
	msgLen, prefixLen := protowire.ConsumeVarint(pbData)
	if prefixLen < 0 {
		return nil, fmt.Errorf("hfile: invalid trailer protobuf length prefix")
	}
	if msgLen > uint64(len(pbData)-prefixLen) {
		return nil, fmt.Errorf("hfile: trailer protobuf length %d exceeds available data", msgLen)
	}

	msgData := pbData[prefixLen : prefixLen+int(msgLen)]
	var pbTrailer pb.FileTrailerProto
	if err := proto.Unmarshal(msgData, &pbTrailer); err != nil {
		return nil, fmt.Errorf("hfile: unmarshal trailer proto: %w", err)
	}

	return &Trailer{
		MajorVersion:              majorVersion,
		MinorVersion:              minorVersion,
		FileInfoOffset:            pbTrailer.GetFileInfoOffset(),
		LoadOnOpenDataOffset:      pbTrailer.GetLoadOnOpenDataOffset(),
		UncompressedDataIndexSize: pbTrailer.GetUncompressedDataIndexSize(),
		TotalUncompressedBytes:    pbTrailer.GetTotalUncompressedBytes(),
		DataIndexCount:            pbTrailer.GetDataIndexCount(),
		MetaIndexCount:            pbTrailer.GetMetaIndexCount(),
		EntryCount:                pbTrailer.GetEntryCount(),
		NumDataIndexLevels:        pbTrailer.GetNumDataIndexLevels(),
		FirstDataBlockOffset:      pbTrailer.GetFirstDataBlockOffset(),
		LastDataBlockOffset:       pbTrailer.GetLastDataBlockOffset(),
		ComparatorClassName:       pbTrailer.GetComparatorClassName(),
		CompressionCodec:          pbTrailer.GetCompressionCodec(),
		EncryptionKey:             pbTrailer.GetEncryptionKey(),
	}, nil
}
