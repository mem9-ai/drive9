package cache

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mem9-ai/drive9/internal/testbackend"
)

func TestCacheStopFencesInFlightPublicationsAndPreservesDiskTier(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f.txt", []byte("persisted-v1"))
	ts := httptest.NewServer(b)
	defer ts.Close()

	dir := t.TempDir()
	first, err := New(Options{Disk: DiskReadCacheOptions{Dir: dir, FreeRatio: -1}})
	require.NoError(t, err)
	ch := newChain(t, ts.URL, first)
	require.Equal(t, "persisted-v1", renameReader(t, ch, "/f.txt"))
	waitDiskFlushed(t, first.dsk)

	preStopEpoch := first.publicationEpoch()
	require.NoError(t, first.Stop(context.Background()))
	require.Greater(t, first.publicationEpoch(), preStopEpoch)
	published := first.publishIfCurrent(preStopEpoch, func() {
		first.mem.Put("/f.txt", []byte("stale"), 1)
	})
	require.False(t, published)
	_, ok := first.mem.Get("/f.txt", 1)
	require.False(t, ok)

	getsBefore := requests(b, "GET", "/f.txt")
	second, err := New(Options{Disk: DiskReadCacheOptions{Dir: dir, FreeRatio: -1}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Stop(context.Background()) })
	ch2 := newChain(t, ts.URL, second)
	require.Equal(t, "persisted-v1", renameReader(t, ch2, "/f.txt"))
	require.Equal(t, getsBefore, requests(b, "GET", "/f.txt"))
}
