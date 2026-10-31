package datastore

// ContentLayout identifies the physical content representation of a file.
type ContentLayout string

const (
	ContentLayoutSingle    ContentLayout = "single"
	ContentLayoutAppendLog ContentLayout = "append_log"
)

func NormalizeContentLayout(layout ContentLayout) ContentLayout {
	switch layout {
	case ContentLayoutAppendLog:
		return ContentLayoutAppendLog
	default:
		return ContentLayoutSingle
	}
}
