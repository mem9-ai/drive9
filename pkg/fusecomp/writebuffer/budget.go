package writebuffer

import (
	"syscall"

	"github.com/mem9-ai/drive9/pkg/fusecomp/membudget"
)

// This file is the buffer's memory-admission surface: charging retained bytes
// to the process budget, handing them back when they are dropped, and settling
// the logical size after a failed write. It is kept apart from the buffer data
// structure so the data-structure file does not grow with budget policy.

// Discard drops *slot, returning its memory-budget charge to the process
// budget. Every site that forgets a handle's buffer must go through here: the
// budget counter is not GC-aware, so a bare `*slot = nil` leaks the charge of
// bytes nobody retains any more.
func Discard(slot **WriteBuffer) {
	if slot == nil || *slot == nil {
		return
	}
	(*slot).Reset()
	*slot = nil
}

// Replace installs next in *slot and returns the previous buffer's charge.
func Replace(slot **WriteBuffer, next *WriteBuffer) {
	if slot == nil {
		return
	}
	if *slot != nil {
		(*slot).Reset()
	}
	*slot = next
}

// SetMemoryBudget wires the process-wide memory budget the buffer charges its
// retained bytes to. It must be called before the first Write/Truncate; a nil
// budget disables accounting.
func (wb *WriteBuffer) SetMemoryBudget(b *membudget.Budget) {
	if wb == nil {
		return
	}
	wb.budget = b
}

// MemoryBudget reports the budget this buffer charges, if any.
func (wb *WriteBuffer) MemoryBudget() *membudget.Budget {
	if wb == nil {
		return nil
	}
	return wb.budget
}

// addMemory charges n bytes against the memory budget and records them.
// It returns syscall.ENOMEM when the budget refuses the allocation, leaving
// the buffer unchanged so the caller can fail the write without corrupting
// the staged image.
func (wb *WriteBuffer) addMemory(n int64) error {
	if wb == nil || n <= 0 {
		return nil
	}
	if wb.budget != nil {
		if err := wb.budget.Acquire(membudget.ClassWriteBuffer, n); err != nil {
			return syscall.ENOMEM
		}
	}
	wb.curMemory += n
	return nil
}

// forceAddMemory records n bytes and charges them even past the budget. It is
// for a caller that cannot proceed without the whole image (a git publish or
// appendlog snapshot that reconstructs the base to write it out): the bytes
// are counted, but refusing them would make the file unpublishable. The
// overrun still squeezes the caches and shows up in the budget snapshot.
func (wb *WriteBuffer) forceAddMemory(n int64) {
	if wb == nil || n <= 0 {
		return
	}
	if wb.budget != nil {
		wb.budget.ForceCharge(membudget.ClassWriteBuffer, n)
	}
	wb.curMemory += n
}

// subMemory drops n bytes of retained memory (releasing the budget charge).
func (wb *WriteBuffer) subMemory(n int64) {
	if wb == nil || n <= 0 {
		return
	}
	wb.curMemory -= n
	if wb.budget != nil {
		wb.budget.Release(membudget.ClassWriteBuffer, n)
	}
}

// rollbackWriteExtent restores the logical size after a failed write attempt:
// the file may only be as long as it was before this call, or as long as the
// bytes this call actually copied — a failed write must not extend the staged
// image over the range it never staged.
func (wb *WriteBuffer) rollbackWriteExtent(prevTotal, offset int64, copied int) {
	if copied > 0 {
		if end := offset + int64(copied); end > prevTotal {
			prevTotal = end
		}
	}
	wb.totalSize = prevTotal
}
