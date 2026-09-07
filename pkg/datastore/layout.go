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
	extentCompactSlices = 350
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
