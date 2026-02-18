package snapshot

import (
	"errors"
	"fmt"
	"io/fs"
	"strings"

	"google.golang.org/protobuf/proto"

	pb "github.com/dethi/riverbed/snapshot/proto"
)

// Manifest holds the parsed content of an HBase snapshot.
type Manifest struct {
	Description *pb.SnapshotDescription
	TableSchema *pb.TableSchema
	Regions     []*pb.SnapshotRegionManifest
}

// ReadManifest reads a snapshot manifest from the given filesystem.
// The filesystem root should be the snapshot directory (containing
// .snapshotinfo and data.manifest or region-manifest.* files).
func ReadManifest(dir fs.FS) (*Manifest, error) {
	// 1. Read .snapshotinfo
	desc, err := readSnapshotInfo(dir)
	if err != nil {
		return nil, err
	}

	// 2. Try consolidated data.manifest first, fall back to individual
	//    region-manifest.* files.
	manifest, err := readDataManifest(dir)
	if err != nil {
		return nil, err
	}
	if manifest != nil {
		return &Manifest{
			Description: desc,
			TableSchema: manifest.TableSchema,
			Regions:     manifest.RegionManifests,
		}, nil
	}

	regions, err := readRegionManifests(dir)
	if err != nil {
		return nil, err
	}
	return &Manifest{
		Description: desc,
		Regions:     regions,
	}, nil
}

func readSnapshotInfo(dir fs.FS) (*pb.SnapshotDescription, error) {
	data, err := fs.ReadFile(dir, ".snapshotinfo")
	if err != nil {
		return nil, fmt.Errorf("snapshot: read .snapshotinfo: %w", err)
	}
	desc := &pb.SnapshotDescription{}
	if err := proto.Unmarshal(data, desc); err != nil {
		return nil, fmt.Errorf("snapshot: parse .snapshotinfo: %w", err)
	}
	return desc, nil
}

func readDataManifest(dir fs.FS) (*pb.SnapshotDataManifest, error) {
	data, err := fs.ReadFile(dir, "data.manifest")
	if err != nil {
		if isNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("snapshot: read data.manifest: %w", err)
	}
	manifest := &pb.SnapshotDataManifest{}
	if err := proto.Unmarshal(data, manifest); err != nil {
		return nil, fmt.Errorf("snapshot: parse data.manifest: %w", err)
	}
	return manifest, nil
}

func readRegionManifests(dir fs.FS) ([]*pb.SnapshotRegionManifest, error) {
	entries, err := fs.ReadDir(dir, ".")
	if err != nil {
		return nil, fmt.Errorf("snapshot: read directory: %w", err)
	}

	var regions []*pb.SnapshotRegionManifest
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), "region-manifest.") {
			continue
		}
		data, err := fs.ReadFile(dir, e.Name())
		if err != nil {
			return nil, fmt.Errorf("snapshot: read %s: %w", e.Name(), err)
		}
		region := &pb.SnapshotRegionManifest{}
		if err := proto.Unmarshal(data, region); err != nil {
			return nil, fmt.Errorf("snapshot: parse %s: %w", e.Name(), err)
		}
		regions = append(regions, region)
	}
	return regions, nil
}

func isNotExist(err error) bool {
	return err != nil && errors.Is(err, fs.ErrNotExist)
}
