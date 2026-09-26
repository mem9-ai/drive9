package fuse

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestDirCacheConditionalDirectoryInstallRejectsRecreatedTarget(t *testing.T) {
	dirCache := NewDirCache(time.Minute)
	installDirectoryPrefetchParent(t, dirCache, "/repo", []CachedFileInfo{
		{Name: "a", IsDir: true, ResourceID: "a-1"},
		{Name: "b", IsDir: true, ResourceID: "b-1"},
	})
	snapshots := dirCache.directoryPrefetchCandidates("/repo/a", 4)
	if len(snapshots) != 1 {
		t.Fatalf("candidates = %+v, want b", snapshots)
	}

	request := dirCache.BeginRequest("/repo/b")
	dirCache.InvalidatePrefix("/repo/b")
	dirCache.Remove("/repo", "b")
	dirCache.Upsert("/repo", CachedFileInfo{Name: "b", IsDir: true, ResourceID: "b-2"})
	_, _, installed := dirCache.putListingIfSnapshot([]CachedFileInfo{{Name: "stale.txt", Revision: 1}}, request, snapshots[0])
	if installed {
		t.Fatal("stale listing installed after target recreation")
	}
	if _, ok := dirCache.Get("/repo/b"); ok {
		t.Fatal("rejected stale listing became cache-visible")
	}
}

func TestDirCacheConditionalDirectoryInstallReturnsFullPartialView(t *testing.T) {
	dirCache := NewNamespaceCache(time.Minute, time.Minute, 2)
	installDirectoryPrefetchParent(t, dirCache, "/repo", []CachedFileInfo{
		{Name: "a", IsDir: true, ResourceID: "a-1"},
		{Name: "b", IsDir: true, ResourceID: "b-1"},
	})
	snapshot, ok := dirCache.directoryPrefetchSnapshot("/repo/b")
	if !ok {
		t.Fatal("missing snapshot for b")
	}
	request := dirCache.BeginRequest("/repo/b")
	view, receipt, installed := dirCache.putListingIfSnapshot([]CachedFileInfo{
		{Name: "1.txt", Revision: 1},
		{Name: "2.txt", Revision: 1},
		{Name: "3.txt", Revision: 1},
	}, request, snapshot)
	if !installed || !receipt.installed {
		t.Fatal("conditional listing was not installed")
	}
	if len(view) != 3 {
		t.Fatalf("full view len = %d, want 3", len(view))
	}
	if _, complete := dirCache.Get("/repo/b"); complete {
		t.Fatal("over-cap listing was cached as complete")
	}
}

func TestDirectoryPrefetchCapsConcurrencyAndReplenishesWithoutRecursion(t *testing.T) {
	var (
		mu        sync.Mutex
		calls     []string
		active    int
		maxActive int
	)
	started := make(chan string, 16)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Query().Get("list") != "1" {
			t.Errorf("unexpected request %s %s?%s", request.Method, request.URL.Path, request.URL.RawQuery)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		mu.Lock()
		calls = append(calls, request.URL.Path)
		active++
		if active > maxActive {
			maxActive = active
		}
		mu.Unlock()
		started <- request.URL.Path
		<-release
		mu.Lock()
		active--
		mu.Unlock()
		writeDirectoryListing(t, w, nil)
	}))
	defer server.Close()

	fs := newDirectoryPrefetchTestFS(server.URL, defaultNamespaceCacheMaxEntries)
	defer fs.OnUnmount()
	installDirectoryPrefetchParent(t, fs.dirCache, "/repo", directoryChildren("a", "b", "c", "d", "e", "f", "g"))
	fs.maybePrefetchSiblingDirectories("/repo/a")
	for range directoryPrefetchConcurrency {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for initial prefetches")
		}
	}
	mu.Lock()
	if maxActive != directoryPrefetchConcurrency {
		mu.Unlock()
		t.Fatalf("max active = %d, want %d", maxActive, directoryPrefetchConcurrency)
	}
	mu.Unlock()
	close(release)
	waitForDirectoryPrefetchIdle(t, fs)

	mu.Lock()
	initialCalls := len(calls)
	mu.Unlock()
	if initialCalls != directoryPrefetchConcurrency {
		t.Fatalf("initial calls = %d, want %d; background prefetch recursed", initialCalls, directoryPrefetchConcurrency)
	}

	release = make(chan struct{})
	fs.maybePrefetchSiblingDirectories("/repo/b")
	for range 2 {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for replenished prefetches")
		}
	}
	close(release)
	waitForDirectoryPrefetchIdle(t, fs)
	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 6 {
		t.Fatalf("total calls = %d, want 6", len(calls))
	}
	if got := strings.Join(calls[4:], ","); got != "/v1/fs/repo/f,/v1/fs/repo/g" && got != "/v1/fs/repo/g,/v1/fs/repo/f" {
		t.Fatalf("replenished calls = %q, want f and g", got)
	}
}

func TestDirectoryPrefetchForegroundJoinsAndReceivesFullListing(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		writeDirectoryListing(t, w, []client.FileInfo{
			{Name: "1.txt", Revision: 1},
			{Name: "2.txt", Revision: 1},
			{Name: "3.txt", Revision: 1},
		})
	}))
	defer server.Close()

	fs := newDirectoryPrefetchTestFS(server.URL, 2)
	defer fs.OnUnmount()
	installDirectoryPrefetchParent(t, fs.dirCache, "/repo", directoryChildren("a", "b"))
	fs.maybePrefetchSiblingDirectories("/repo/a")
	<-started

	result := make(chan []CachedFileInfo, 1)
	errs := make(chan error, 1)
	go func() {
		entries, err := fs.loadForegroundRemoteDirectory(context.Background(), "/repo/b", fs.mountViewGeneration.Load())
		result <- entries
		errs <- err
	}()
	waitForDirectoryPrefetchJoined(t, fs, "/repo/b")
	close(release)
	if err := <-errs; err != nil {
		t.Fatalf("foreground load error = %v", err)
	}
	if entries := <-result; len(entries) != 3 {
		t.Fatalf("foreground entries len = %d, want 3", len(entries))
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("LIST calls = %d, want 1 shared request", got)
	}
	if _, complete := fs.dirCache.Get("/repo/b"); complete {
		t.Fatal("over-cap listing was cached as complete")
	}
}

func TestDirectoryPrefetchForegroundOwnRequestDoesNotUsePrefetchTimeout(t *testing.T) {
	oldTimeout := directoryPrefetchTimeout
	directoryPrefetchTimeout = 10 * time.Millisecond
	t.Cleanup(func() { directoryPrefetchTimeout = oldTimeout })

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		time.Sleep(40 * time.Millisecond)
		writeDirectoryListing(t, w, []client.FileInfo{{Name: "slow.txt", Revision: 1}})
	}))
	defer server.Close()

	fs := newDirectoryPrefetchTestFS(server.URL, defaultNamespaceCacheMaxEntries)
	defer fs.OnUnmount()
	installDirectoryPrefetchParent(t, fs.dirCache, "/repo", directoryChildren("a", "b"))
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	entries, err := fs.loadForegroundRemoteDirectory(ctx, "/repo/b", fs.mountViewGeneration.Load())
	if err != nil {
		t.Fatalf("slow foreground load error = %v", err)
	}
	if len(entries) != 1 || entries[0].Name != "slow.txt" {
		t.Fatalf("slow foreground entries = %+v, want slow.txt", entries)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("LIST calls = %d, want one foreground request", got)
	}
}

func TestDirectoryPrefetchForegroundOwnRequestSurvivesInitiatorCancellation(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		started <- struct{}{}
		<-release
		writeDirectoryListing(t, w, []client.FileInfo{{Name: "shared.txt", Revision: 1}})
	}))
	defer server.Close()

	fs := newDirectoryPrefetchTestFS(server.URL, defaultNamespaceCacheMaxEntries)
	defer fs.OnUnmount()
	installDirectoryPrefetchParent(t, fs.dirCache, "/repo", directoryChildren("a", "b"))

	initiatorCtx, cancelInitiator := context.WithCancel(context.Background())
	initiatorErr := make(chan error, 1)
	go func() {
		_, err := fs.loadForegroundRemoteDirectory(initiatorCtx, "/repo/b", fs.mountViewGeneration.Load())
		initiatorErr <- err
	}()
	<-started
	cancelInitiator()
	if err := <-initiatorErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("initiator error = %v, want context canceled", err)
	}

	result := make(chan []CachedFileInfo, 1)
	joinerErr := make(chan error, 1)
	go func() {
		entries, err := fs.loadForegroundRemoteDirectory(context.Background(), "/repo/b", fs.mountViewGeneration.Load())
		result <- entries
		joinerErr <- err
	}()
	waitForDirectoryPrefetchJoined(t, fs, "/repo/b")
	close(release)
	if err := <-joinerErr; err != nil {
		t.Fatalf("joiner error = %v", err)
	}
	if entries := <-result; len(entries) != 1 || entries[0].Name != "shared.txt" {
		t.Fatalf("joiner entries = %+v, want shared.txt", entries)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("LIST calls = %d, want one shared request", got)
	}
}

func TestDirectoryPrefetchForegroundRechecksCompletedCache(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if calls.Add(1) == 1 {
			started <- struct{}{}
			<-release
		}
		writeDirectoryListing(t, w, []client.FileInfo{{Name: "cached.txt", Revision: 1}})
	}))
	defer server.Close()

	fs := newDirectoryPrefetchTestFS(server.URL, defaultNamespaceCacheMaxEntries)
	defer fs.OnUnmount()
	installDirectoryPrefetchParent(t, fs.dirCache, "/repo", directoryChildren("a", "b"))
	fs.maybePrefetchSiblingDirectories("/repo/a")
	<-started
	if _, ok := fs.dirCache.Get("/repo/b"); ok {
		t.Fatal("target listing unexpectedly cached before background completion")
	}
	close(release)
	waitForDirectoryPrefetchIdle(t, fs)

	entries, err := fs.loadForegroundRemoteDirectory(context.Background(), "/repo/b", fs.mountViewGeneration.Load())
	if err != nil {
		t.Fatalf("foreground cache recheck error = %v", err)
	}
	if len(entries) != 1 || entries[0].Name != "cached.txt" {
		t.Fatalf("foreground cache recheck entries = %+v, want cached.txt", entries)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("LIST calls = %d, want completed prefetch reused", got)
	}
}

func TestDirectoryPrefetchScheduleRechecksCompletedCache(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		writeDirectoryListing(t, w, nil)
	}))
	defer server.Close()

	fs := newDirectoryPrefetchTestFS(server.URL, defaultNamespaceCacheMaxEntries)
	defer fs.OnUnmount()
	installDirectoryPrefetchParent(t, fs.dirCache, "/repo", directoryChildren("a", "b"))
	snapshot, ok := fs.dirCache.directoryPrefetchSnapshot("/repo/b")
	if !ok {
		t.Fatal("missing snapshot for b")
	}
	request := fs.dirCache.BeginRequest("/repo/b")
	fs.dirCache.PutListing("/repo/b", []CachedFileInfo{{Name: "cached.txt", Revision: 1}}, request)

	scheduled, full := fs.directoryPrefetch.schedule(fs, snapshot, fs.mountViewGeneration.Load(), fs.statCacheTrustEpoch.Load())
	if scheduled || full {
		t.Fatalf("schedule = %v full=%v, want completed cache skipped", scheduled, full)
	}
	time.Sleep(10 * time.Millisecond)
	if got := calls.Load(); got != 0 {
		t.Fatalf("LIST calls = %d, want no redundant background request", got)
	}
}

func TestDirectoryPrefetchCanceledForegroundDoesNotFallback(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		started <- struct{}{}
		<-release
		writeDirectoryListing(t, w, nil)
	}))
	defer server.Close()

	fs := newDirectoryPrefetchTestFS(server.URL, defaultNamespaceCacheMaxEntries)
	defer fs.OnUnmount()
	installDirectoryPrefetchParent(t, fs.dirCache, "/repo", directoryChildren("a", "b"))
	fs.maybePrefetchSiblingDirectories("/repo/a")
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := fs.loadForegroundRemoteDirectory(ctx, "/repo/b", fs.mountViewGeneration.Load())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("foreground error = %v, want context canceled", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("LIST calls = %d, want no fallback", got)
	}
	close(release)
	waitForDirectoryPrefetchIdle(t, fs)
	if entries, ok := fs.dirCache.Get("/repo/b"); !ok || len(entries) != 0 {
		t.Fatalf("shared prefetch after waiter cancellation = %+v complete=%v, want complete empty listing", entries, ok)
	}
}

func TestDirectoryPrefetchRejectsRecreatedTargetDuringRequest(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		started <- struct{}{}
		<-release
		writeDirectoryListing(t, w, []client.FileInfo{{Name: "stale.txt", Revision: 1}})
	}))
	defer server.Close()

	fs := newDirectoryPrefetchTestFS(server.URL, defaultNamespaceCacheMaxEntries)
	defer fs.OnUnmount()
	installDirectoryPrefetchParent(t, fs.dirCache, "/repo", directoryChildren("a", "b"))
	fs.maybePrefetchSiblingDirectories("/repo/a")
	<-started
	fs.dirCache.InvalidatePrefix("/repo/b")
	fs.dirCache.Remove("/repo", "b")
	fs.dirCache.Upsert("/repo", CachedFileInfo{Name: "b", IsDir: true, ResourceID: "b-new"})
	close(release)
	waitForDirectoryPrefetchIdle(t, fs)
	if _, ok := fs.dirCache.Get("/repo/b"); ok {
		t.Fatal("stale recreated-target result installed")
	}
}

func TestDirectoryPrefetchRejectsLocalChildMutationDuringRequest(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		started <- struct{}{}
		<-release
		writeDirectoryListing(t, w, []client.FileInfo{{Name: "stale.txt", Revision: 1}})
	}))
	defer server.Close()

	fs := newDirectoryPrefetchTestFS(server.URL, defaultNamespaceCacheMaxEntries)
	defer fs.OnUnmount()
	installDirectoryPrefetchParent(t, fs.dirCache, "/repo", directoryChildren("a", "b"))
	fs.maybePrefetchSiblingDirectories("/repo/a")
	<-started
	fs.dirCache.Upsert("/repo/b", CachedFileInfo{Name: "local.txt", Revision: 2})
	close(release)
	waitForDirectoryPrefetchIdle(t, fs)

	lookup := fs.dirCache.Lookup("/repo/b", "local.txt")
	if lookup.kind != namespaceLookupPositive || lookup.item.Revision != 2 {
		t.Fatalf("local child lookup = %+v, want revision 2", lookup)
	}
	if stale := fs.dirCache.Lookup("/repo/b", "stale.txt"); stale.kind == namespaceLookupPositive {
		t.Fatalf("stale child lookup = %+v, want rejected response", stale)
	}
}

func TestDirectoryPrefetchRejectsRemoteChildMutationDuringRequest(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		started <- struct{}{}
		<-release
		writeDirectoryListing(t, w, []client.FileInfo{{Name: "stale.txt", Revision: 1}})
	}))
	defer server.Close()

	fs := newDirectoryPrefetchTestFS(server.URL, defaultNamespaceCacheMaxEntries)
	defer fs.OnUnmount()
	installDirectoryPrefetchParent(t, fs.dirCache, "/repo", directoryChildren("a", "b"))
	fs.maybePrefetchSiblingDirectories("/repo/a")
	<-started
	watcher := &SSEWatcher{fs: fs, actor: "this-mount"}
	watcher.handleChange(&client.ChangeEvent{Path: "/repo/b/remote.txt", Op: "write", Actor: "other-mount"})
	close(release)
	waitForDirectoryPrefetchIdle(t, fs)

	if _, ok := fs.dirCache.Get("/repo/b"); ok {
		t.Fatal("in-flight result installed after remote child mutation")
	}
}

func TestDirectoryPrefetchDifferentIdentityDoesNotJoinOldRequest(t *testing.T) {
	oldStarted := make(chan struct{}, 1)
	oldRelease := make(chan struct{})
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if calls.Add(1) == 1 {
			oldStarted <- struct{}{}
			<-oldRelease
			writeDirectoryListing(t, w, []client.FileInfo{{Name: "stale.txt", Revision: 1}})
			return
		}
		writeDirectoryListing(t, w, []client.FileInfo{{Name: "fresh.txt", Revision: 2}})
	}))
	defer server.Close()

	fs := newDirectoryPrefetchTestFS(server.URL, defaultNamespaceCacheMaxEntries)
	defer fs.OnUnmount()
	installDirectoryPrefetchParent(t, fs.dirCache, "/repo", directoryChildren("a", "b"))
	fs.maybePrefetchSiblingDirectories("/repo/a")
	<-oldStarted
	fs.dirCache.Remove("/repo", "b")
	fs.dirCache.Upsert("/repo", CachedFileInfo{Name: "b", IsDir: true, ResourceID: "b-new"})

	entries, err := fs.loadForegroundRemoteDirectory(context.Background(), "/repo/b", fs.mountViewGeneration.Load())
	if err != nil {
		t.Fatalf("new-identity foreground load error = %v", err)
	}
	if len(entries) != 1 || entries[0].Name != "fresh.txt" {
		t.Fatalf("new-identity foreground entries = %+v, want fresh.txt", entries)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("LIST calls = %d, want separate old and new identity calls", got)
	}
	close(oldRelease)
	waitForDirectoryPrefetchIdle(t, fs)
	cached, ok := fs.dirCache.Get("/repo/b")
	if !ok || len(cached) != 1 || cached[0].Name != "fresh.txt" {
		t.Fatalf("cached listing = %+v complete=%v, want fresh identity only", cached, ok)
	}
}

func TestDirectoryPrefetchRejectsSSEDisconnectDuringRequest(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		started <- struct{}{}
		<-release
		writeDirectoryListing(t, w, nil)
	}))
	defer server.Close()

	fs := newDirectoryPrefetchTestFS(server.URL, defaultNamespaceCacheMaxEntries)
	defer fs.OnUnmount()
	installDirectoryPrefetchParent(t, fs.dirCache, "/repo", directoryChildren("a", "b"))
	fs.maybePrefetchSiblingDirectories("/repo/a")
	<-started
	fs.markStatCacheUnverified()
	close(release)
	waitForDirectoryPrefetchIdle(t, fs)
	if _, ok := fs.dirCache.Get("/repo/b"); ok {
		t.Fatal("in-flight result installed after SSE disconnect")
	}
}

func TestDirectoryPrefetchRejectsRequestAcrossSSEReconnect(t *testing.T) {
	oldStarted := make(chan struct{}, 1)
	oldRelease := make(chan struct{})
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if calls.Add(1) == 1 {
			oldStarted <- struct{}{}
			<-oldRelease
			writeDirectoryListing(t, w, []client.FileInfo{{Name: "stale.txt", Revision: 1}})
			return
		}
		writeDirectoryListing(t, w, []client.FileInfo{{Name: "fresh.txt", Revision: 2}})
	}))
	defer server.Close()

	fs := newDirectoryPrefetchTestFS(server.URL, defaultNamespaceCacheMaxEntries)
	defer fs.OnUnmount()
	installDirectoryPrefetchParent(t, fs.dirCache, "/repo", directoryChildren("a", "b"))
	fs.maybePrefetchSiblingDirectories("/repo/a")
	<-oldStarted
	fs.markStatCacheUnverified()
	fs.markStatCacheVerified()

	result := make(chan []CachedFileInfo, 1)
	errs := make(chan error, 1)
	go func() {
		entries, err := fs.loadForegroundRemoteDirectory(context.Background(), "/repo/b", fs.mountViewGeneration.Load())
		result <- entries
		errs <- err
	}()
	select {
	case err := <-errs:
		if err != nil {
			close(oldRelease)
			t.Fatalf("post-reconnect foreground load error = %v", err)
		}
	case <-time.After(2 * time.Second):
		close(oldRelease)
		t.Fatal("post-reconnect foreground joined the pre-disconnect request")
	}
	entries := <-result
	if len(entries) != 1 || entries[0].Name != "fresh.txt" {
		close(oldRelease)
		t.Fatalf("post-reconnect entries = %+v, want fresh.txt", entries)
	}
	close(oldRelease)
	waitForDirectoryPrefetchIdle(t, fs)
	if got := calls.Load(); got != 2 {
		t.Fatalf("LIST calls = %d, want old and post-reconnect requests", got)
	}
	cached, ok := fs.dirCache.Get("/repo/b")
	if !ok || len(cached) != 1 || cached[0].Name != "fresh.txt" {
		t.Fatalf("cached listing = %+v complete=%v, want fresh post-reconnect result", cached, ok)
	}
}

func TestDirectoryPrefetchDoesNotScheduleWhileSSEUnverified(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		writeDirectoryListing(t, w, nil)
	}))
	defer server.Close()

	fs := newDirectoryPrefetchTestFS(server.URL, defaultNamespaceCacheMaxEntries)
	defer fs.OnUnmount()
	installDirectoryPrefetchParent(t, fs.dirCache, "/repo", directoryChildren("a", "b"))
	fs.markStatCacheUnverified()
	fs.maybePrefetchSiblingDirectories("/repo/a")
	time.Sleep(10 * time.Millisecond)

	if got := calls.Load(); got != 0 {
		t.Fatalf("LIST calls = %d, want none while SSE is unverified", got)
	}
}

func TestCodingAgentListDirBypassesCachedListingWhileSSEUnverified(t *testing.T) {
	var listCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Query().Get("list") != "1" {
			t.Errorf("unexpected request %s %s?%s", request.Method, request.URL.Path, request.URL.RawQuery)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		listCalls.Add(1)
		writeDirectoryListing(t, w, []client.FileInfo{{Name: "fresh.txt", Revision: 2}})
	}))
	defer server.Close()

	fs := newDirectoryPrefetchTestFS(server.URL, defaultNamespaceCacheMaxEntries)
	defer fs.OnUnmount()
	fs.dirCache.Put("/repo", []CachedFileInfo{{Name: "stale.txt", Revision: 1}})
	fs.markStatCacheUnverified()

	entries, err := fs.listDir(context.Background(), "/repo")
	if err != nil {
		t.Fatalf("listDir: %v", err)
	}
	if got := listCalls.Load(); got != 1 {
		t.Fatalf("LIST calls = %d, want 1 while SSE is unverified", got)
	}
	if len(entries) != 1 || entries[0].Name != "fresh.txt" {
		t.Fatalf("entries = %+v, want fresh.txt", entries)
	}
}

func TestDirectoryPrefetchUnmountCancelsSharedRequest(t *testing.T) {
	started := make(chan struct{}, 1)
	canceled := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		started <- struct{}{}
		<-request.Context().Done()
		canceled <- struct{}{}
	}))
	defer server.Close()

	fs := newDirectoryPrefetchTestFS(server.URL, defaultNamespaceCacheMaxEntries)
	installDirectoryPrefetchParent(t, fs.dirCache, "/repo", directoryChildren("a", "b"))
	fs.maybePrefetchSiblingDirectories("/repo/a")
	<-started

	done := make(chan struct{})
	go func() {
		fs.OnUnmount()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("OnUnmount did not wait for canceled prefetch")
	}
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("shared request context was not canceled")
	}
	if _, ok := fs.dirCache.Get("/repo/b"); ok {
		t.Fatal("canceled unmount prefetch installed a listing")
	}
	fs.OnUnmount()
}

func TestDirectoryPrefetchRejectsMountResetDuringRequest(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		started <- struct{}{}
		<-release
		writeDirectoryListing(t, w, nil)
	}))
	defer server.Close()

	fs := newDirectoryPrefetchTestFS(server.URL, defaultNamespaceCacheMaxEntries)
	defer fs.OnUnmount()
	installDirectoryPrefetchParent(t, fs.dirCache, "/repo", directoryChildren("a", "b"))
	fs.maybePrefetchSiblingDirectories("/repo/a")
	<-started
	fs.resetMountView()
	close(release)
	waitForDirectoryPrefetchIdle(t, fs)
	if _, ok := fs.dirCache.Get("/repo/b"); ok {
		t.Fatal("in-flight result installed after mount reset")
	}
}

func TestDirectoryPrefetchFailureFallsBackForLiveForeground(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		call := calls.Add(1)
		if call == 1 {
			started <- struct{}{}
			<-release
			http.Error(w, "temporary", http.StatusServiceUnavailable)
			return
		}
		writeDirectoryListing(t, w, []client.FileInfo{{Name: "fresh.txt", Revision: 1}})
	}))
	defer server.Close()

	fs := newDirectoryPrefetchTestFS(server.URL, defaultNamespaceCacheMaxEntries)
	defer fs.OnUnmount()
	installDirectoryPrefetchParent(t, fs.dirCache, "/repo", directoryChildren("a", "b"))
	fs.maybePrefetchSiblingDirectories("/repo/a")
	<-started

	result := make(chan []CachedFileInfo, 1)
	errs := make(chan error, 1)
	go func() {
		entries, err := fs.loadForegroundRemoteDirectory(context.Background(), "/repo/b", fs.mountViewGeneration.Load())
		result <- entries
		errs <- err
	}()
	waitForDirectoryPrefetchJoined(t, fs, "/repo/b")
	close(release)
	if err := <-errs; err != nil {
		t.Fatalf("foreground fallback error = %v", err)
	}
	if entries := <-result; len(entries) != 1 || entries[0].Name != "fresh.txt" {
		t.Fatalf("foreground fallback entries = %+v, want fresh.txt", entries)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("LIST calls = %d, want failed prefetch plus foreground fallback", got)
	}
}

func TestDirectoryPrefetchOversizeFallsBackToUnboundedForeground(t *testing.T) {
	largeName := strings.Repeat("x", directoryPrefetchMaxResponseBytes+1024)
	started := make(chan struct{}, 1)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if calls.Add(1) == 1 {
			started <- struct{}{}
		}
		writeDirectoryListing(t, w, []client.FileInfo{{Name: largeName, Revision: 1}})
	}))
	defer server.Close()

	fs := newDirectoryPrefetchTestFS(server.URL, defaultNamespaceCacheMaxEntries)
	defer fs.OnUnmount()
	installDirectoryPrefetchParent(t, fs.dirCache, "/repo", directoryChildren("a", "b"))
	fs.maybePrefetchSiblingDirectories("/repo/a")
	<-started
	waitForDirectoryPrefetchIdle(t, fs)
	if _, ok := fs.dirCache.Get("/repo/b"); ok {
		t.Fatal("oversized prefetch installed a partial listing")
	}

	entries, err := fs.loadForegroundRemoteDirectory(context.Background(), "/repo/b", fs.mountViewGeneration.Load())
	if err != nil {
		t.Fatalf("unbounded foreground load error = %v", err)
	}
	if len(entries) != 1 || entries[0].Name != largeName {
		t.Fatalf("foreground entries = %d, want complete oversized listing", len(entries))
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("LIST calls = %d, want bounded prefetch plus foreground fallback", got)
	}
}

func installDirectoryPrefetchParent(t *testing.T, dirCache *DirCache, parentPath string, children []CachedFileInfo) {
	t.Helper()
	request := dirCache.BeginRequest(parentPath)
	view := dirCache.PutListing(parentPath, children, request)
	if len(view) != len(children) {
		t.Fatalf("installed parent view len = %d, want %d", len(view), len(children))
	}
}

func directoryChildren(names ...string) []CachedFileInfo {
	children := make([]CachedFileInfo, len(names))
	for i, name := range names {
		children[i] = CachedFileInfo{Name: name, IsDir: true, ResourceID: name + "-resource"}
	}
	return children
}

func newDirectoryPrefetchTestFS(serverURL string, maxEntries int) *Dat9FS {
	opts := &MountOptions{
		Profile:            MountProfileCodingAgent,
		DirCacheMaxEntries: maxEntries,
		PerfCounters:       true,
	}
	opts.setDefaults()
	return NewDat9FS(newTestClient(serverURL), opts)
}

func writeDirectoryListing(t *testing.T, writer http.ResponseWriter, entries []client.FileInfo) {
	t.Helper()
	writer.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(writer).Encode(map[string]any{"entries": entries}); err != nil {
		t.Errorf("encode listing: %v", err)
	}
}

func waitForDirectoryPrefetchIdle(t *testing.T, fs *Dat9FS) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		fs.directoryPrefetch.mu.Lock()
		remaining := len(fs.directoryPrefetch.calls)
		fs.directoryPrefetch.mu.Unlock()
		if remaining == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("directory prefetch still has %d calls", remaining)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitForDirectoryPrefetchJoined(t *testing.T, fs *Dat9FS, targetPath string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		joined := false
		fs.directoryPrefetch.mu.Lock()
		for key, call := range fs.directoryPrefetch.calls {
			if key.path == targetPath && call.joined {
				joined = true
				break
			}
		}
		fs.directoryPrefetch.mu.Unlock()
		if joined {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("foreground did not join directory prefetch for %s", targetPath)
		}
		time.Sleep(time.Millisecond)
	}
}
