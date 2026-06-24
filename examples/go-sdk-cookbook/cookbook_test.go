package go_sdk_cookbook_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	drive9 "github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/journal"
)

func ExampleClient_constructionStatusAndRawRequests() {
	ctx := context.Background()

	c := drive9.New("https://drive9.example.com", "owner-or-fs-scoped-api-key")
	_ = drive9.NewWithToken("https://drive9.example.com", "delegated-vault-jwt")

	c.SetActor("agent-a")
	_ = c.BaseURL()
	_ = c.APIKey()

	c.Warm(ctx)
	_ = c.MaxUploadBytes(ctx)
	_ = c.SmallFileThreshold(ctx)
	_ = c.CachedSmallFileThreshold()
	c.SetSmallFileThresholdForTests(50_000)

	resp, err := c.RawPost("/v1/custom/action", strings.NewReader(`{"ok":true}`))
	if err == nil {
		_ = resp.Body.Close()
	}
	resp, err = c.RawDelete("/v1/custom/resource", strings.NewReader(`{"reason":"cleanup"}`))
	if err == nil {
		_ = resp.Body.Close()
	}
	resp, err = c.RawGet("/v1/status")
	if err == nil {
		_ = resp.Body.Close()
	}
}

func ExampleClient_filesystemCRUDAndMetadata() {
	ctx := context.Background()
	c := drive9.New("https://drive9.example.com", "api-key")

	_ = c.Mkdir("/workspace")
	_ = c.MkdirCtx(ctx, "/workspace/", 0o755)

	_ = c.Write("/workspace/a.txt", []byte("a"))
	_ = c.WriteCtx(ctx, "/workspace/b.txt", []byte("b"))
	_ = c.WriteCtxConditional(ctx, "/workspace/state.json", []byte("{}"), -1)
	_ = c.WriteCtxConditionalWithTags(ctx, "/workspace/tagged.txt", []byte("tagged"), -1, map[string]string{"kind": "note"})
	_ = c.WriteCtxConditionalWithDescription(ctx, "/workspace/described.txt", []byte("body"), -1, "short description")
	_, _ = c.WriteCtxConditionalWithRevision(ctx, "/workspace/revision.txt", []byte("body"), -1)

	_, _ = c.CreateFile("/workspace/empty.txt")
	_, _ = c.CreateFileCtx(ctx, "/workspace/empty-ctx.txt")

	_, _ = c.Read("/workspace/a.txt")
	_, _ = c.ReadCtx(ctx, "/workspace/b.txt")
	_, _ = c.ReadAt("/workspace/big.bin", 0, 1024)
	_, _ = c.ReadAtCtx(ctx, "/workspace/big.bin", 1024, 2048)

	_, _ = c.List("/workspace/")
	_, _ = c.ListCtx(ctx, "/workspace/")
	_, _ = c.BatchStatCtx(ctx, []string{"/workspace/a.txt", "/workspace/b.txt"})
	_, _ = c.BatchReadSmallCtx(ctx, []string{"/workspace/a.txt"}, 1<<20)

	_, _ = c.Stat("/workspace/a.txt")
	_, _ = c.StatCtx(ctx, "/workspace/a.txt")
	_, _ = c.StatMetadata("/workspace/a.txt")
	_, _ = c.StatMetadataCtx(ctx, "/workspace/a.txt")
	_, _ = c.StatMetadataCompat("/workspace/a.txt")
	_, _ = c.StatMetadataCompatCtx(ctx, "/workspace/a.txt")

	_ = c.Copy("/workspace/a.txt", "/workspace/a-copy.txt")
	_ = c.CopyCtx(ctx, "/workspace/a.txt", "/workspace/a-copy-ctx.txt")
	_ = c.Rename("/workspace/a-copy.txt", "/workspace/a-renamed.txt")
	_ = c.RenameCtx(ctx, "/workspace/a-copy-ctx.txt", "/workspace/a-renamed-ctx.txt")
	_ = c.Symlink("a.txt", "/workspace/a-link")
	_ = c.SymlinkCtx(ctx, "a.txt", "/workspace/a-link-ctx")
	_ = c.Hardlink("/workspace/a.txt", "/workspace/a-hardlink.txt")
	_ = c.HardlinkCtx(ctx, "/workspace/a.txt", "/workspace/a-hardlink-ctx.txt")
	_ = c.Chmod("/workspace/a.txt", 0o640)
	_ = c.ChmodCtx(ctx, "/workspace/a.txt", 0o644)

	_, _ = c.SQL("select path, size_bytes from files limit 10")
	_, _ = c.Grep("deployment checklist", "/workspace/", 20)
	_, _ = c.GrepWithLayer("deployment checklist", "/workspace/", 20, "layer-123")

	params := url.Values{}
	params.Set("name", "*.md")
	params.Set("tag", "kind=note")
	_, _ = c.Find("/workspace/", params)

	_ = c.Delete("/workspace/a-renamed.txt")
	_ = c.DeleteCtx(ctx, "/workspace/a-renamed-ctx.txt")
	_ = c.DeleteFileCtx(ctx, "/workspace/a.txt")
	_ = c.DeleteDirCtx(ctx, "/workspace/empty-dir/")
	_ = c.RemoveAll("/workspace/tmp/")
	_ = c.RemoveAllCtx(ctx, "/workspace/tmp-ctx/")
}

func ExampleClient_transfersAppendPatchAndStreaming() {
	ctx := context.Background()
	c := drive9.New("https://drive9.example.com", "api-key")
	body := []byte("payload")
	reader := bytes.NewReader(body)

	_ = c.WriteStream(ctx, "/uploads/payload.bin", bytes.NewReader(body), int64(len(body)), nil)
	_ = c.WriteStreamWithTags(ctx, "/uploads/tagged.bin", bytes.NewReader(body), int64(len(body)), nil, map[string]string{"kind": "artifact"})
	_, _ = c.WriteStreamWithSummary(
		ctx,
		"/uploads/summary.bin",
		bytes.NewReader(body),
		int64(len(body)),
		nil,
		drive9.WithTags(map[string]string{"kind": "summary"}),
		drive9.WithDescription("summary upload"),
	)
	_, _ = c.WriteStreamWithSummaryAndTags(ctx, "/uploads/tag-summary.bin", bytes.NewReader(body), int64(len(body)), nil, map[string]string{"kind": "summary"})
	_, _ = c.WriteStreamWithSummaryAndDescription(ctx, "/uploads/desc-summary.bin", bytes.NewReader(body), int64(len(body)), nil, "description")
	_ = c.WriteStreamConditional(ctx, "/uploads/cas.bin", bytes.NewReader(body), int64(len(body)), nil, 12)
	_ = c.WriteMultipartStreamConditional(ctx, "/uploads/forced-multipart.bin", reader, int64(len(body)), nil, 12)

	rc, err := c.ReadStream(ctx, "/uploads/payload.bin")
	if err == nil {
		_ = rc.Close()
	}
	target, err := c.ResolveReadTarget(ctx, "/uploads/large.bin")
	if err == nil {
		rc, err = c.ReadObjectRange(ctx, target, 0, 1<<20)
		if err == nil {
			_ = rc.Close()
		}
	}
	rc, err = c.ReadStreamRange(ctx, "/uploads/large.bin", 0, 1<<20)
	if err == nil {
		_ = rc.Close()
	}
	_ = c.DownloadToFile(ctx, "/uploads/large.bin", "./large.bin", 10<<20)
	_, _ = c.DownloadToFileWithSummary(ctx, "/uploads/large.bin", "./large.bin", 10<<20)

	_ = c.ResumeUpload(ctx, "/uploads/resume.bin", bytes.NewReader(body), int64(len(body)), nil)
	_ = c.ResumeUploadWithTags(ctx, "/uploads/resume-tags.bin", bytes.NewReader(body), int64(len(body)), nil, map[string]string{"kind": "resume"})
	_, _ = c.ResumeUploadWithSummary(ctx, "/uploads/resume-summary.bin", bytes.NewReader(body), int64(len(body)), nil)
	_, _ = c.ResumeUploadWithSummaryAndTags(ctx, "/uploads/resume-summary-tags.bin", bytes.NewReader(body), int64(len(body)), nil, map[string]string{"kind": "resume"})

	_ = c.AppendStream(ctx, "/logs/events.log", strings.NewReader("line\n"), int64(len("line\n")), nil)
	_ = c.PatchFile(
		ctx,
		"/uploads/large.bin",
		20<<20,
		[]int{1, 3},
		func(partNumber int, partSize int64, origData []byte) ([]byte, error) {
			return bytes.Repeat([]byte{byte(partNumber)}, int(partSize)), nil
		},
		nil,
		drive9.WithPartSize(8<<20),
		drive9.WithExpectedRevision(12),
	)

	sw := c.NewStreamWriter(ctx, "/streams/object.bin", int64(len(body)))
	_ = sw.Started()
	_ = sw.WritePart(ctx, 1, body)
	_ = sw.Complete(ctx, 0, nil)
	_ = sw.Abort(ctx)

	conditional := c.NewStreamWriterConditional(ctx, "/streams/cas.bin", int64(len(body)), 12)
	_ = conditional.Abort(ctx)
	described := c.NewStreamWriterWithDescription(ctx, "/streams/desc.bin", int64(len(body)), "stream upload")
	_ = described.Abort(ctx)
}

func ExampleClient_tokensAndVault() {
	ctx := context.Background()
	owner := drive9.New("https://drive9.example.com", "owner-api-key")

	issued, err := owner.IssueScopedToken(ctx, drive9.IssueScopedTokenRequest{
		Subject:    "agent-a",
		TTLSeconds: int64((30 * time.Minute).Seconds()),
		Scopes: []drive9.FSScopeGrant{
			{Prefix: "/workspace/run-42/", Ops: []string{"read", "write"}},
		},
	})
	if err == nil {
		_ = owner.RevokeScopedToken(ctx, issued.TokenID)
		_ = owner.RevokeScopedTokenByAPIKey(ctx, issued.Token)
	}

	_, _ = owner.CreateVaultSecret(ctx, "prod-db", map[string]string{"DB_URL": "postgres://..."})
	_, _ = owner.UpdateVaultSecret(ctx, "prod-db", map[string]string{"DB_URL": "postgres://rotated"})
	_, _ = owner.ListVaultSecrets(ctx)
	_, _ = owner.ReadVaultSecretAsOwner(ctx, "prod-db")
	_, _ = owner.ReadVaultSecretFieldAsOwner(ctx, "prod-db", "DB_URL")
	_, _ = owner.IssueVaultToken(ctx, "agent-a", "task-1", []string{"prod-db/DB_URL"}, time.Hour)
	_ = owner.RevokeVaultToken(ctx, "token-id")
	grant, err := owner.IssueVaultGrant(ctx, drive9.VaultGrantIssueRequest{
		Agent:      "agent-a",
		Scope:      []string{"prod-db/DB_URL"},
		Perm:       "read",
		TTLSeconds: 3600,
		LabelHint:  "prod-db-url",
	})
	if err == nil {
		_ = owner.RevokeVaultGrant(ctx, grant.GrantID, "owner", "done")
	}
	_, _ = owner.QueryVaultAudit(ctx, "prod-db", 100)

	delegated := drive9.NewWithToken("https://drive9.example.com", "delegated-vault-jwt")
	_, _ = delegated.ListReadableVaultSecrets(ctx)
	_, _ = delegated.ReadVaultSecret(ctx, "prod-db")
	_, _ = delegated.ReadVaultSecretField(ctx, "prod-db", "DB_URL")
	_ = owner.DeleteVaultSecret(ctx, "prod-db")
}

func ExampleClient_quota() {
	ctx := context.Background()
	serverURL := "https://drive9.example.com"

	tidbCloudPublicKey := "tidbcloud-public-key"
	tidbCloudPrivateKey := "tidbcloud-private-key"
	tenantID := "tnt_abc123"
	credentialClient := drive9.New(serverURL, "")

	tenant, err := credentialClient.AdminGetTenant(ctx, drive9.QuotaRequest{
		TenantID:   tenantID,
		PublicKey:  tidbCloudPublicKey,
		PrivateKey: tidbCloudPrivateKey,
	})
	if err == nil && tenant.Quota != nil {
		quota := tenant.Quota
		_ = quota.Config.MaxStorageSize
		_ = quota.Config.MaxFileSize
		_ = quota.Config.MaxFileCount
		_ = quota.Config.TiDBCloudSpendingLimit
		_ = quota.Usage.FileCount
	}

	storageSize := int64(100 * 1024) // Mi
	fileSize := int64(1024)          // Mi
	fileCount := int64(100000)
	spendingLimit := int64(10000)
	_, _ = credentialClient.AdminSetTenantQuota(ctx, drive9.QuotaSetRequest{
		TenantID:               tenantID,
		PublicKey:              tidbCloudPublicKey,
		PrivateKey:             tidbCloudPrivateKey,
		MaxStorageSize:         &storageSize,
		MaxFileSize:            &fileSize,
		MaxFileCount:           &fileCount,
		TiDBCloudSpendingLimit: &spendingLimit,
	})
}

func ExampleClient_eventsLayersGitAndJournal() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := drive9.New("https://drive9.example.com", "api-key")

	go c.WatchEvents(ctx, "agent-a", func(change *drive9.ChangeEvent, reset *drive9.ResetEvent) {})
	go c.WatchEventsWithLifecycle(ctx, "agent-a", func(change *drive9.ChangeEvent, reset *drive9.ResetEvent) {}, drive9.EventLifecycle{
		OnCurrent:      func(seq uint64) {},
		OnDisconnected: func(err error) {},
	})

	layer, err := c.CreateFSLayer(ctx, drive9.FSLayerCreateRequest{
		BaseRootPath: "/workspace/",
		Name:         "agent-a-work",
		ActorID:      "agent-a",
	})
	if err == nil {
		_, _ = c.ListFSLayers(ctx)
		_, _ = c.GetFSLayer(ctx, layer.LayerID)
		_, _ = c.UpsertFSLayerEntry(ctx, layer.LayerID, drive9.FSLayerEntryRequest{
			Path:        "/workspace/note.txt",
			Op:          "upsert",
			Kind:        "file",
			Content:     []byte("draft"),
			ContentType: "text/plain",
			SizeBytes:   int64(len("draft")),
			Mode:        0o644,
		})
		_, _ = c.UploadFSLayerFile(ctx, layer.LayerID, "/workspace/blob.bin", bytes.NewReader([]byte("blob")), int64(len("blob")), 0, 0o644, true)
		_, _ = c.ReadFSLayerFile(ctx, layer.LayerID, "/workspace/blob.bin", nil)
		rc, err := c.ReadFSLayerFileStream(ctx, layer.LayerID, "/workspace/blob.bin", nil)
		if err == nil {
			_ = rc.Close()
		}
		_, _ = c.GetFSLayerEntry(ctx, layer.LayerID, "/workspace/blob.bin")
		_, _ = c.GetFSLayerEntryAtSeq(ctx, layer.LayerID, "/workspace/blob.bin", 1)
		_, _ = c.DiffFSLayer(ctx, layer.LayerID)
		_, _ = c.DiffFSLayerAtSeq(ctx, layer.LayerID, 1)
		_, _ = c.ReplayFSLayer(ctx, layer.LayerID)
		_, _ = c.ReplayFSLayerAtSeq(ctx, layer.LayerID, 1)
		checkpoint, err := c.CheckpointFSLayer(ctx, layer.LayerID, drive9.FSLayerCheckpointRequest{Label: "before-commit"})
		if err == nil {
			_, _ = c.GetFSLayerCheckpoint(ctx, checkpoint.CheckpointID)
		}
		_, _ = c.ListFSLayerEvents(ctx, layer.LayerID, 0)
		_ = c.RollbackFSLayer(ctx, layer.LayerID)
		_, _ = c.CommitFSLayer(ctx, layer.LayerID)
	}

	ws, err := c.UpsertGitWorkspace(ctx, drive9.GitWorkspaceRequest{
		RootPath:   "/repo/",
		RepoURL:    "https://github.com/mem9-ai/drive9.git",
		RemoteName: "origin",
		BranchName: "main",
	})
	if err == nil {
		_, _ = c.GetGitWorkspaceByRoot(ctx, "/repo/")
		_, _ = c.GetGitWorkspace(ctx, ws.WorkspaceID)
		_, _ = c.ListGitWorkspaces(ctx)
		_ = c.ReplaceGitTree(ctx, ws.WorkspaceID, drive9.GitTreeReplaceRequest{
			CommitSHA: "abc123",
			Nodes: []drive9.GitTreeNode{{
				Path:      "README.md",
				Name:      "README.md",
				Kind:      "blob",
				Mode:      "100644",
				ObjectSHA: "blob-sha",
				SizeBytes: 9,
			}},
		})
		_, _ = c.ListGitTree(ctx, ws.WorkspaceID, "abc123")
		_, _ = c.UpsertGitState(ctx, ws.WorkspaceID, drive9.GitStateRequest{
			CheckpointCommit: "abc123",
			Content:          []byte("git state"),
		})
		_, _ = c.GetGitState(ctx, ws.WorkspaceID)
		pack, err := c.PutGitObjectPack(ctx, ws.WorkspaceID, drive9.GitObjectPackRequest{Content: []byte("pack")})
		if err == nil {
			_, _ = c.GetGitObjectPack(ctx, ws.WorkspaceID, pack.PackID)
		}
		_, _ = c.ListGitObjectPacks(ctx, ws.WorkspaceID)
		_, _ = c.PutGitOverlayEntry(ctx, ws.WorkspaceID, drive9.GitOverlayEntryRequest{
			Path:          "README.md",
			Op:            "upsert",
			Kind:          "blob",
			Content:       []byte("overlay"),
			SizeBytes:     7,
			BaseObjectSHA: "blob-sha",
		})
		_, _ = c.GetGitOverlayEntry(ctx, ws.WorkspaceID, "README.md")
		_, _ = c.ListGitOverlayEntries(ctx, ws.WorkspaceID)
		_ = c.DeleteGitWorkspace(ctx, ws.WorkspaceID)
	}

	j, err := c.CreateJournal(ctx, journal.CreateRequest{
		Kind:  "agent",
		Title: "run-42",
		Actor: journal.Actor{Type: "agent", ID: "agent-a"},
	})
	if err == nil {
		_, _ = c.AppendJournalEntries(ctx, j.JournalID, "append-1", []journal.EntryInput{{
			Type:     "note",
			Status:   "done",
			Actor:    journal.Actor{Type: "agent", ID: "agent-a"},
			Subjects: []string{"run-42"},
			Summary:  json.RawMessage(`{"text":"completed step"}`),
		}})
		_, _ = c.ReadJournalEntries(ctx, j.JournalID, 0, 100)
		_, _ = c.SearchJournal(ctx, journal.SearchRequest{
			Type:     "note",
			Subjects: []string{"run-42"},
			Limit:    20,
		})
		_, _ = c.VerifyJournal(ctx, j.JournalID)
	}
}

func Example_errorHelpers() {
	var err error = &drive9.StatusError{StatusCode: http.StatusNotFound, Message: "not found"}
	_ = drive9.IsNotFound(err)

	err = &drive9.StatusError{StatusCode: http.StatusConflict, Message: "conflict"}
	_ = errors.Is(err, drive9.ErrConflict)

	_ = drive9.IsPresignExpired(err)
	_ = drive9.IsReadTargetNoRedirect(err)
}

func Example_localFileTransferShape() {
	ctx := context.Background()
	c := drive9.New("https://drive9.example.com", "api-key")

	f, err := os.Open("artifact.bin")
	if err != nil {
		return
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return
	}
	_, _ = c.WriteStreamWithSummary(ctx, "/artifacts/artifact.bin", f, info.Size(), func(part, total int, bytesUploaded int64) {})

	rc, err := c.ReadStream(ctx, "/artifacts/artifact.bin")
	if err != nil {
		return
	}
	defer func() { _ = rc.Close() }()
	_, _ = io.Copy(io.Discard, rc)
}

var coveredClientMethods = map[string]bool{
	"AppendJournalEntries":                 true,
	"AppendStream":                         true,
	"APIKey":                               true,
	"BaseURL":                              true,
	"BatchReadSmallCtx":                    true,
	"BatchStatCtx":                         true,
	"CachedSmallFileThreshold":             true,
	"CheckpointFSLayer":                    true,
	"Chmod":                                true,
	"ChmodCtx":                             true,
	"CommitFSLayer":                        true,
	"Copy":                                 true,
	"CopyCtx":                              true,
	"CreateFile":                           true,
	"CreateFileCtx":                        true,
	"CreateFSLayer":                        true,
	"CreateJournal":                        true,
	"CreateVaultSecret":                    true,
	"Delete":                               true,
	"DeleteCtx":                            true,
	"DeleteDirCtx":                         true,
	"DeleteFileCtx":                        true,
	"DeleteGitWorkspace":                   true,
	"DeleteVaultSecret":                    true,
	"DiffFSLayer":                          true,
	"DiffFSLayerAtSeq":                     true,
	"DownloadToFile":                       true,
	"DownloadToFileWithSummary":            true,
	"Find":                                 true,
	"GetFSLayer":                           true,
	"GetFSLayerCheckpoint":                 true,
	"GetFSLayerEntry":                      true,
	"GetFSLayerEntryAtSeq":                 true,
	"GetGitObjectPack":                     true,
	"GetGitOverlayEntry":                   true,
	"GetGitState":                          true,
	"GetGitWorkspace":                      true,
	"GetGitWorkspaceByRoot":                true,
	"GetQuota":                             true,
	"Grep":                                 true,
	"GrepWithLayer":                        true,
	"Hardlink":                             true,
	"HardlinkCtx":                          true,
	"IssueScopedToken":                     true,
	"IssueVaultGrant":                      true,
	"IssueVaultToken":                      true,
	"List":                                 true,
	"ListCtx":                              true,
	"ListFSLayerEvents":                    true,
	"ListFSLayers":                         true,
	"ListGitObjectPacks":                   true,
	"ListGitOverlayEntries":                true,
	"ListGitTree":                          true,
	"ListGitWorkspaces":                    true,
	"ListReadableVaultSecrets":             true,
	"ListVaultSecrets":                     true,
	"MaxUploadBytes":                       true,
	"Mkdir":                                true,
	"MkdirCtx":                             true,
	"NewStreamWriter":                      true,
	"NewStreamWriterConditional":           true,
	"NewStreamWriterWithDescription":       true,
	"PatchFile":                            true,
	"PutGitObjectPack":                     true,
	"PutGitOverlayEntry":                   true,
	"QueryVaultAudit":                      true,
	"RawDelete":                            true,
	"RawGet":                               true,
	"RawPost":                              true,
	"Read":                                 true,
	"ReadAt":                               true,
	"ReadAtCtx":                            true,
	"ReadCtx":                              true,
	"ReadFSLayerFile":                      true,
	"ReadFSLayerFileStream":                true,
	"ReadJournalEntries":                   true,
	"ReadObjectRange":                      true,
	"ReadStream":                           true,
	"ReadStreamRange":                      true,
	"ReadVaultSecret":                      true,
	"ReadVaultSecretAsOwner":               true,
	"ReadVaultSecretField":                 true,
	"ReadVaultSecretFieldAsOwner":          true,
	"ReplaceGitTree":                       true,
	"RemoveAll":                            true,
	"RemoveAllCtx":                         true,
	"Rename":                               true,
	"RenameCtx":                            true,
	"ReplayFSLayer":                        true,
	"ReplayFSLayerAtSeq":                   true,
	"ResolveReadTarget":                    true,
	"ResumeUpload":                         true,
	"ResumeUploadWithSummary":              true,
	"ResumeUploadWithSummaryAndTags":       true,
	"ResumeUploadWithTags":                 true,
	"RevokeScopedToken":                    true,
	"RevokeScopedTokenByAPIKey":            true,
	"RevokeVaultGrant":                     true,
	"RevokeVaultToken":                     true,
	"RollbackFSLayer":                      true,
	"SQL":                                  true,
	"SearchJournal":                        true,
	"SetActor":                             true,
	"SetQuota":                             true,
	"SetSmallFileThresholdForTests":        true,
	"SmallFileThreshold":                   true,
	"Stat":                                 true,
	"StatCtx":                              true,
	"StatMetadata":                         true,
	"StatMetadataCompat":                   true,
	"StatMetadataCompatCtx":                true,
	"StatMetadataCtx":                      true,
	"Symlink":                              true,
	"SymlinkCtx":                           true,
	"UpdateVaultSecret":                    true,
	"UploadFSLayerFile":                    true,
	"UpsertFSLayerEntry":                   true,
	"UpsertGitState":                       true,
	"UpsertGitWorkspace":                   true,
	"VerifyJournal":                        true,
	"Warm":                                 true,
	"WatchEvents":                          true,
	"WatchEventsWithLifecycle":             true,
	"Write":                                true,
	"WriteCtx":                             true,
	"WriteCtxConditional":                  true,
	"WriteCtxConditionalWithDescription":   true,
	"WriteCtxConditionalWithRevision":      true,
	"WriteCtxConditionalWithTags":          true,
	"WriteMultipartStreamConditional":      true,
	"WriteStream":                          true,
	"WriteStreamConditional":               true,
	"WriteStreamWithSummary":               true,
	"WriteStreamWithSummaryAndDescription": true,
	"WriteStreamWithSummaryAndTags":        true,
	"WriteStreamWithTags":                  true,
}

func TestClientMethodExampleCoverage(t *testing.T) {
	clientType := reflect.TypeOf((*drive9.Client)(nil))
	var missing []string
	for i := 0; i < clientType.NumMethod(); i++ {
		method := clientType.Method(i)
		if method.PkgPath != "" {
			continue
		}
		if !coveredClientMethods[method.Name] {
			missing = append(missing, method.Name)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("SDK cookbook missing Client method examples: %s", strings.Join(missing, ", "))
	}
}

func TestStreamWriterMethodExampleCoverage(t *testing.T) {
	covered := map[string]bool{
		"Abort":     true,
		"Complete":  true,
		"Started":   true,
		"WritePart": true,
	}
	streamType := reflect.TypeOf((*drive9.StreamWriter)(nil))
	var missing []string
	for i := 0; i < streamType.NumMethod(); i++ {
		method := streamType.Method(i)
		if method.PkgPath != "" {
			continue
		}
		if !covered[method.Name] {
			missing = append(missing, method.Name)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("SDK cookbook missing StreamWriter method examples: %s", strings.Join(missing, ", "))
	}
}
