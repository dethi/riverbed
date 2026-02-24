// Package storage provides unified file access for both local filesystem
// paths and Google Cloud Storage paths (gs://bucket/object).
package storage

import (
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// ParseGCSPath parses a GCS path of the form gs://bucket/object and returns
// the bucket and object. The object may be empty if the path refers to a
// bucket root.
func ParseGCSPath(path string) (bucket, object string, ok bool) {
	after, found := strings.CutPrefix(path, "gs://")
	if !found {
		return "", "", false
	}
	bucket, object, _ = strings.Cut(after, "/")
	if bucket == "" {
		return "", "", false
	}
	return bucket, object, true
}

// JoinPath joins a base path with sub-path elements, correctly handling both
// local filesystem paths and GCS paths (gs://bucket/prefix).
func JoinPath(base string, elems ...string) string {
	if bucket, object, ok := ParseGCSPath(base); ok {
		parts := make([]string, 0, len(elems)+1)
		if object != "" {
			parts = append(parts, object)
		}
		parts = append(parts, elems...)
		return "gs://" + bucket + "/" + strings.Join(parts, "/")
	}
	return filepath.Join(append([]string{base}, elems...)...)
}

// OpenFile opens a file at the given path (local or gs://) and returns an
// io.ReaderAt, the file size, and a close function. The caller must call
// close when done.
func OpenFile(ctx context.Context, path string) (io.ReaderAt, int64, func(), error) {
	if bucket, object, ok := ParseGCSPath(path); ok {
		return openGCSObject(ctx, bucket, object)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, nil, err
	}
	return f, fi.Size(), func() { f.Close() }, nil
}

// OpenDir opens a directory at the given path (local or gs://) and returns
// an fs.FS rooted at that directory.
func OpenDir(ctx context.Context, path string) (fs.FS, error) {
	if bucket, object, ok := ParseGCSPath(path); ok {
		return openGCSDir(ctx, bucket, object)
	}
	return os.DirFS(path), nil
}
