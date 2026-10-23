// Handle-local state machine for the append protocol: tail safety of the
// committed baseline plus the SQLite WAL header coverage that gates a
// generation reset.
package appendlog

import (
	"github.com/mem9-ai/drive9/pkg/client"
)

type appendLogHandleState struct {
	initialized bool
	appendSafe  bool
	unsupported bool
	// sqliteWALConfirmed carries a proof that the committed baseline starts
	// with a valid WAL header (captured before a write could overwrite it);
	// sqliteWALCommittedHeader is that header, the salt baseline a
	// generation reset must differ from.
	sqliteWALConfirmed       bool
	sqliteWALCommittedHeader sqliteWALHeader
	sqliteWALTruncated       bool
	sqliteWALWriteAtZero     bool
	// sqliteWALWriteBeyondHeader fences reset to a header-only fsync. Any
	// frame or other user write in this dirty generation requires a full image.
	sqliteWALWriteBeyondHeader bool
	// sqliteWALHeaderDirtyByteMask tracks coverage of [0, 32) by the current
	// dirty generation. It prevents a partial header fsync from rotating a WAL.
	sqliteWALHeaderDirtyByteMask uint32
	layout                       client.ContentLayout
	revision                     int64
	size                         int64
	rewriteBaseRevision          int64
	rewriteBaseSize              int64
	hasRewriteBase               bool
}

func (state *appendLogHandleState) recordSQLiteWALHeaderWrite(offset, written int64) {
	if state == nil || written <= 0 {
		return
	}
	if offset+written > sqliteWALHeaderSize {
		state.sqliteWALWriteBeyondHeader = true
	}
	if offset >= sqliteWALHeaderSize || offset+written <= 0 {
		return
	}
	if offset == 0 {
		state.sqliteWALWriteAtZero = true
	}
	start := offset
	if start < 0 {
		start = 0
	}
	end := offset + written
	if end > sqliteWALHeaderSize {
		end = sqliteWALHeaderSize
	}
	for index := start; index < end; index++ {
		state.sqliteWALHeaderDirtyByteMask |= uint32(1) << uint(index)
	}
}

func (state *appendLogHandleState) clearSQLiteWALHeaderWrite() {
	if state == nil {
		return
	}
	state.sqliteWALWriteAtZero = false
	state.sqliteWALHeaderDirtyByteMask = 0
	state.sqliteWALWriteBeyondHeader = false
}

func (state *appendLogHandleState) sqliteWALHeaderDirtyComplete() bool {
	return state != nil && state.sqliteWALWriteAtZero && state.sqliteWALHeaderDirtyByteMask == ^uint32(0)
}
