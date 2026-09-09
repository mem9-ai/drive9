package datastore

// ContentLayout identifies the physical content representation of a file.
type ContentLayout string

const (
	ContentLayoutSingle    ContentLayout = "single"
	ContentLayoutAppendLog ContentLayout = "append_log"
	ContentLayoutExtent    ContentLayout = "extent"
)

const (
	// ExtentChunkSize matches JuiceFS ChunkSize (64 MiB).
	ExtentChunkSize = 1 << 26
	// ExtentBlockSize matches JuiceFS default block size (4 MiB).
	ExtentBlockSize = 4 << 20
	extentSliceBytes = 24
	// JuiceFS maxSlices (pkg/meta/base.go) is 2500; below that compact is
	// async and must not block Write. Enqueue at 350 made HTTP compact CAS
	// contend with sqlite exclusive COMMITs (JuiceFS SQL compact is µs).
	extentCompactSlices = 2500
)

func NormalizeContentLayout(layout ContentLayout) ContentLayout {
	switch layout {
	case ContentLayoutAppendLog:
		return ContentLayoutAppendLog
	case ContentLayoutExtent:
		return ContentLayoutExtent
	default:
		return ContentLayoutSingle
	}
}

const StorageExtent StorageType = "extent"
