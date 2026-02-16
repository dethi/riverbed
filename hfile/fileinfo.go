package hfile

import (
	"fmt"
	"io"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	pb "github.com/dethi/riverbed/hfile/proto"
)

// pbMagic is the 4-byte magic prefix HBase writes before protobuf-encoded data.
var pbMagic = [4]byte{'P', 'B', 'U', 'F'}

// Well-known FileInfo keys.
const (
	FileInfoAvgKeyLen         = "hfile.AVG_KEY_LEN"
	FileInfoAvgValueLen       = "hfile.AVG_VALUE_LEN"
	FileInfoLastKey           = "hfile.LASTKEY"
	FileInfoMaxMemstoreTS     = "MAX_MEMSTORE_TS_KEY"
	FileInfoDataBlockEncoding = "DATA_BLOCK_ENCODING"
)

// ReadFileInfo reads the FILE_INFO block at the given offset and returns the key-value map.
func ReadFileInfo(r io.ReaderAt, offset int64, decomp Decompressor) (map[string][]byte, error) {
	blk, err := ReadBlock(r, offset, decomp)
	if err != nil {
		return nil, fmt.Errorf("hfile: read file info block: %w", err)
	}
	if blk.Header.Type != BlockFileInfo {
		return nil, fmt.Errorf("hfile: expected FILE_INFO block, got %s", blk.Header.Type)
	}
	return parseFileInfo(blk.Data)
}

func parseFileInfo(data []byte) (map[string][]byte, error) {
	// HBase writes a 4-byte "PBUF" magic before the protobuf-encoded data.
	if len(data) < 4 {
		return nil, fmt.Errorf("hfile: file info data too short")
	}
	if [4]byte(data[:4]) != pbMagic {
		return nil, fmt.Errorf("hfile: file info missing PBUF magic, got %q", data[:4])
	}
	data = data[4:]

	// The protobuf is written in delimited format (varint length prefix + message).
	msgLen, prefixLen := protowire.ConsumeVarint(data)
	if prefixLen < 0 {
		return nil, fmt.Errorf("hfile: invalid file info protobuf length prefix")
	}
	msgData := data[prefixLen : prefixLen+int(msgLen)]

	var fi pb.FileInfoProto
	if err := proto.Unmarshal(msgData, &fi); err != nil {
		return nil, fmt.Errorf("hfile: unmarshal file info: %w", err)
	}
	m := make(map[string][]byte, len(fi.MapEntry))
	for _, entry := range fi.MapEntry {
		m[string(entry.First)] = entry.Second
	}
	return m, nil
}
