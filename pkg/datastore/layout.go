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
	ExtentBlockSize  = 4 << 20
	extentSliceBytes = 24
	// JuiceFS maxSlices (pkg/meta/base.go) is 2500; below that compact is
	// async and must not block Write. Enqueue at 350 made HTTP compact CAS
	// contend with sqlite exclusive COMMITs (JuiceFS SQL compact is µs).
	extentCompactSlices = 2500
	// extentBlockGCGraceSQL is how long a queued block deletion waits before
	// the GC consumer may delete the object. The superseded slice disappears
	// from metadata the moment compaction commits, so another mount that still
	// holds the pre-compaction slice list would read a deleted object if the
	// delete ran immediately (JuiceFS documents exactly this hazard for
	// compaction combined with a write cache). It is SQL rather than a
	// Duration because both enqueue sites build the INSERT text directly and
	// TiDB evaluates it server-side, so no clock skew is involved.
	extentBlockGCGraceSQL = "DATE_ADD(CURRENT_TIMESTAMP(3), INTERVAL 60 SECOND)"
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
