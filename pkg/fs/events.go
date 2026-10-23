package fs

import (
	"fmt"
	"sync"
)

// EventKind is the typed event vocabulary shared by plugins and the kernel
// facade. Producers do not know their consumers: a durability layer emits
// Landed, read-plane caches seed from it, the identity table advances
// revisions on it.
type EventKind uint8

// Events delivered over the EventBus.
const (
	EventWrite       EventKind = iota // a path was mutated through this mount
	EventDelete                       // a path was removed through this mount
	EventCheckout                     // the mount switched to a different view/revision
	EventStaged                       // dirty bytes were staged locally, not yet committed
	EventLanded                       // a staged mutation was acknowledged by the remote
	EventConflicted                   // a commit lost a revision race
	EventInvalidated                  // remote-origin change invalidated local knowledge
)

// String returns the event kind's name.
func (k EventKind) String() string {
	switch k {
	case EventWrite:
		return "write"
	case EventDelete:
		return "delete"
	case EventCheckout:
		return "checkout"
	case EventStaged:
		return "staged"
	case EventLanded:
		return "landed"
	case EventConflicted:
		return "conflicted"
	case EventInvalidated:
		return "invalidated"
	}
	return fmt.Sprintf("event(%d)", uint8(k))
}

// Event is the typed notification delivered over the EventBus.
type Event struct {
	Kind EventKind // event discriminator
	// RenamedTo carries the destination of a successful rename (the
	// rename's EventWrite emission): lets plane-local stores follow the
	// move without intercepting the dispatch.
	RenamedTo string
	Path      string // canonical absolute path the event concerns
	Rev       int64  // EventLanded: the revision the remote acknowledged
	Size      int64  // EventLanded: the landed size
	// HasSize distinguishes a landed image of size 0 (an emptied file) from
	// an event that carries no size at all: readers adopt Size as their new
	// identity only when this is set.
	HasSize bool
	Epoch   uint64 // mutation epoch captured at handoff (see InodeTable)
	// KernelInvalidate asks the facade to flush the KERNEL caches for every
	// inode in addition to the userspace tiers (a 401 view reset: the
	// credential may now address a different namespace). A plain SSE
	// reconnect/reset leaves it false — pages stay valid there.
	KernelInvalidate bool
}

// EventBus delivers typed events between plugins and the kernel facade.
type EventBus struct {
	mu     sync.Mutex
	nextID uint64
	subs   []eventSub
}

type eventSub struct {
	id    uint64
	kinds uint64
	fn    func(Event)
}

// NewEventBus creates an empty bus.
func NewEventBus() *EventBus { return &EventBus{} }

// On subscribes fn to one event kind and returns an unsubscribe func.
func (b *EventBus) On(kind EventKind, fn func(Event)) (off func()) {
	if b == nil || fn == nil {
		return func() {}
	}
	b.mu.Lock()
	b.nextID++
	sub := eventSub{id: b.nextID, kinds: 1 << uint64(kind), fn: fn}
	b.subs = append(b.subs, sub)
	b.mu.Unlock()
	return func() {
		b.mu.Lock()
		for i := range b.subs {
			if b.subs[i].id == sub.id {
				b.subs = append(b.subs[:i], b.subs[i+1:]...)
				break
			}
		}
		b.mu.Unlock()
	}
}

// Emit delivers ev to every subscriber of its kind. Handlers run without
// the bus lock held, so a handler may subscribe or emit recursively.
func (b *EventBus) Emit(ev Event) {
	if b == nil {
		return
	}
	b.mu.Lock()
	subs := make([]eventSub, len(b.subs))
	copy(subs, b.subs)
	b.mu.Unlock()
	mask := uint64(1) << uint64(ev.Kind)
	for _, s := range subs {
		if s.kinds&mask != 0 {
			s.fn(ev)
		}
	}
}
