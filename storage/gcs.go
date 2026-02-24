package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"sync"
	"time"

	gcs "cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

var (
	gcsOnce   sync.Once
	gcsClient *gcs.Client
	gcsErr    error
)

func getGCSClient() (*gcs.Client, error) {
	gcsOnce.Do(func() {
		gcsClient, gcsErr = gcs.NewClient(context.Background())
	})
	return gcsClient, gcsErr
}

// gcsReaderAt implements io.ReaderAt for a GCS object using range reads.
type gcsReaderAt struct {
	ctx    context.Context
	bucket string
	object string
}

func openGCSObject(ctx context.Context, bucket, object string) (io.ReaderAt, int64, func(), error) {
	client, err := getGCSClient()
	if err != nil {
		return nil, 0, nil, fmt.Errorf("gcs: create client: %w", err)
	}
	attrs, err := client.Bucket(bucket).Object(object).Attrs(ctx)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("gcs: gs://%s/%s: %w", bucket, object, err)
	}
	r := &gcsReaderAt{ctx: ctx, bucket: bucket, object: object}
	return r, attrs.Size, func() {}, nil
}

func (r *gcsReaderAt) ReadAt(p []byte, off int64) (int, error) {
	client, err := getGCSClient()
	if err != nil {
		return 0, err
	}
	rc, err := client.Bucket(r.bucket).Object(r.object).NewRangeReader(r.ctx, off, int64(len(p)))
	if err != nil {
		return 0, fmt.Errorf("gcs: range read gs://%s/%s at offset %d: %w", r.bucket, r.object, off, err)
	}
	defer rc.Close()
	return io.ReadFull(rc, p)
}

// gcsFS implements fs.FS for a GCS bucket + prefix.
type gcsFS struct {
	ctx    context.Context
	bucket string
	prefix string // always ends with "/" if non-empty
}

func openGCSDir(ctx context.Context, bucket, prefix string) (fs.FS, error) {
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return &gcsFS{ctx: ctx, bucket: bucket, prefix: prefix}, nil
}

func (fsys *gcsFS) Open(name string) (fs.File, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrInvalid}
	}
	if name == "." {
		return &gcsDir{fsys: fsys}, nil
	}
	return &gcsFile{
		ctx:    fsys.ctx,
		bucket: fsys.bucket,
		object: fsys.prefix + name,
		name:   name,
	}, nil
}

// gcsFile implements fs.File for a single GCS object. The underlying reader
// is opened lazily on the first Read or Stat call.
type gcsFile struct {
	ctx    context.Context
	bucket string
	object string
	name   string

	once sync.Once
	rc   *gcs.Reader
	err  error
}

func (f *gcsFile) open() error {
	f.once.Do(func() {
		client, err := getGCSClient()
		if err != nil {
			f.err = err
			return
		}
		rc, err := client.Bucket(f.bucket).Object(f.object).NewReader(f.ctx)
		if err != nil {
			if errors.Is(err, gcs.ErrObjectNotExist) {
				f.err = &fs.PathError{Op: "open", Path: f.name, Err: fs.ErrNotExist}
			} else {
				f.err = fmt.Errorf("gcs: open gs://%s/%s: %w", f.bucket, f.object, err)
			}
			return
		}
		f.rc = rc
	})
	return f.err
}

func (f *gcsFile) Read(p []byte) (int, error) {
	if err := f.open(); err != nil {
		return 0, err
	}
	return f.rc.Read(p)
}

func (f *gcsFile) Close() error {
	if f.rc != nil {
		return f.rc.Close()
	}
	return nil
}

func (f *gcsFile) Stat() (fs.FileInfo, error) {
	if err := f.open(); err != nil {
		return nil, err
	}
	// rc.Attrs is populated from HTTP response headers when the reader opens.
	return &gcsFileInfo{
		name:    f.name,
		size:    f.rc.Attrs.Size,
		modTime: f.rc.Attrs.LastModified,
	}, nil
}

// gcsDir implements fs.ReadDirFile for listing objects under a GCS prefix.
type gcsDir struct {
	fsys *gcsFS
}

func (d *gcsDir) Read([]byte) (int, error) {
	return 0, &fs.PathError{Op: "read", Path: ".", Err: fs.ErrInvalid}
}

func (d *gcsDir) Close() error { return nil }

func (d *gcsDir) Stat() (fs.FileInfo, error) {
	return &gcsFileInfo{name: ".", isDir: true}, nil
}

func (d *gcsDir) ReadDir(n int) ([]fs.DirEntry, error) {
	client, err := getGCSClient()
	if err != nil {
		return nil, err
	}
	query := &gcs.Query{
		Prefix:    d.fsys.prefix,
		Delimiter: "/",
	}
	it := client.Bucket(d.fsys.bucket).Objects(d.fsys.ctx, query)

	var entries []fs.DirEntry
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("gcs: list gs://%s/%s: %w", d.fsys.bucket, d.fsys.prefix, err)
		}
		if attrs.Prefix != "" {
			// Common prefix → treat as a subdirectory.
			name := strings.TrimSuffix(strings.TrimPrefix(attrs.Prefix, d.fsys.prefix), "/")
			entries = append(entries, &gcsDirEntry{name: name, isDir: true})
		} else {
			name := strings.TrimPrefix(attrs.Name, d.fsys.prefix)
			entries = append(entries, &gcsDirEntry{
				name:    name,
				size:    attrs.Size,
				modTime: attrs.Updated,
			})
		}
		if n > 0 && len(entries) >= n {
			break
		}
	}
	return entries, nil
}

// gcsFileInfo implements fs.FileInfo.
type gcsFileInfo struct {
	name    string
	size    int64
	modTime time.Time
	isDir   bool
}

func (fi *gcsFileInfo) Name() string { return fi.name }
func (fi *gcsFileInfo) Size() int64  { return fi.size }
func (fi *gcsFileInfo) Mode() fs.FileMode {
	if fi.isDir {
		return fs.ModeDir | 0o555
	}
	return 0o444
}
func (fi *gcsFileInfo) ModTime() time.Time { return fi.modTime }
func (fi *gcsFileInfo) IsDir() bool        { return fi.isDir }
func (fi *gcsFileInfo) Sys() any           { return nil }

// gcsDirEntry implements fs.DirEntry.
type gcsDirEntry struct {
	name    string
	size    int64
	modTime time.Time
	isDir   bool
}

func (de *gcsDirEntry) Name() string { return de.name }
func (de *gcsDirEntry) IsDir() bool  { return de.isDir }
func (de *gcsDirEntry) Type() fs.FileMode {
	if de.isDir {
		return fs.ModeDir
	}
	return 0
}
func (de *gcsDirEntry) Info() (fs.FileInfo, error) {
	return &gcsFileInfo{
		name:    de.name,
		size:    de.size,
		modTime: de.modTime,
		isDir:   de.isDir,
	}, nil
}
