package datastore

import "testing"

func TestJfsEnsureInitSkippedWhenReady(t *testing.T) {
	s := &Store{}
	s.jfsInited.Store(true)
	// tx is unused once the root row has been seen; a nil tx must not panic.
	if err := s.jfsEnsureInitTx(nil); err != nil {
		t.Fatalf("jfsEnsureInitTx: %v", err)
	}
}
