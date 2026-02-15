package hfile

import (
	"fmt"
	"io"

	"google.golang.org/protobuf/proto"

	pb "github.com/dethi/riverbed/hfile/proto"
)

// Well-known FileInfo keys.
const (
	FileInfoAvgKeyLen          = "hfile.AVG_KEY_LEN"
	FileInfoAvgValueLen        = "hfile.AVG_VALUE_LEN"
	FileInfoLastKey            = "hfile.LASTKEY"
	FileInfoMaxMemstoreTS      = "MAX_MEMSTORE_TS_KEY"
	FileInfoDataBlockEncoding  = "hfile.DATA_BLOCK_ENCODING"
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
	var fi pb.FileInfoProto
	if err := proto.Unmarshal(data, &fi); err != nil {
		return nil, fmt.Errorf("hfile: unmarshal file info: %w", err)
	}
	m := make(map[string][]byte, len(fi.MapEntry))
	for _, entry := range fi.MapEntry {
		m[string(entry.First)] = entry.Second
	}
	return m, nil
}
