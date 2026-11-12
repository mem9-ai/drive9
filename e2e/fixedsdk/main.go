// Command fixedsdk exercises the real SDK against the paired server test fixture.
// It starts no service and accepts only an unauthenticated loopback endpoint.
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

func localEndpoint(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "http" || u.User != nil || u.Port() == "" || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return false
	}
	ip := net.ParseIP(u.Hostname())
	return ip != nil && ip.IsLoopback()
}

func readEquals(ctx context.Context, c *client.Client, path string, want []byte) bool {
	r, err := c.ReadStream(ctx, path)
	if err != nil {
		return false
	}
	got, err := io.ReadAll(io.LimitReader(r, int64(len(want))+1))
	closeErr := r.Close()
	return err == nil && closeErr == nil && bytes.Equal(got, want)
}

func createWork(ctx context.Context, c *client.Client, layer, parent string) (client.ViewWork, *client.Client, error) {
	if _, err := c.CreateFixedView(ctx, client.FixedViewCreate{LayerID: layer, ParentLayerID: parent, OperationID: "create-" + layer}); err != nil {
		return client.ViewWork{}, nil, errors.New("create view")
	}
	claim, err := c.BeginViewWork(ctx, layer, "work-"+layer, time.Hour, false)
	if err != nil || !claim.Launch {
		return client.ViewWork{}, nil, errors.New("begin work")
	}
	bound, err := c.WithView(claim.Work.Binding())
	return claim.Work, bound, err
}

func run(ctx context.Context, base string) error {
	if !localEndpoint(base) {
		return errors.New("invalid loopback endpoint")
	}
	c := client.New(base, "")
	seed := []byte("main seed")
	if err := c.WriteCtx(ctx, "/file", seed); err != nil {
		return errors.New("main seed")
	}
	work, view, err := createWork(ctx, c, "sdk-a", "")
	if err != nil {
		return err
	}
	before, err := view.StatCtx(ctx, "/file")
	if err != nil || before.Revision < 1 {
		return errors.New("inherited stat")
	}
	// Two parts, including bytes across the exact 4 MiB boundary.
	data := bytes.Repeat([]byte("a"), (4<<20)+37)
	copy(data[(4<<20)-4:], []byte("part-boundary"))
	writeCtx, err := client.WithViewOperationID(ctx, "write-large")
	if err != nil {
		return errors.New("publication identity")
	}
	if err := view.WriteStreamConditional(writeCtx, "/file", bytes.NewReader(data), int64(len(data)), nil, before.Revision); err != nil {
		return errors.New("large stream write")
	}
	if !readEquals(ctx, view, "/file", data) || !readEquals(ctx, c, "/file", seed) {
		return errors.New("uploaded bytes or main isolation")
	}
	chunk, err := view.ReadAtCtx(ctx, "/file", (4<<20)-4, 20)
	if err != nil || !bytes.Equal(chunk, data[(4<<20)-4:(4<<20)+16]) {
		return errors.New("cross-part range read")
	}
	receipt, err := c.GetFixedViewOperation(ctx, work.LayerID, "write-large")
	if err != nil || receipt.State != "DONE" {
		return errors.New("publication receipt")
	}
	_, child, err := createWork(ctx, c, "sdk-child", work.LayerID)
	if err != nil {
		return err
	}
	current, err := view.StatCtx(ctx, "/file")
	if err != nil {
		return errors.New("current stat")
	}
	next := append([]byte(nil), data...)
	next[len(next)-1] = 'b'
	overwriteCtx, _ := client.WithViewOperationID(ctx, "overwrite-large")
	if err := view.WriteStreamConditional(overwriteCtx, "/file", bytes.NewReader(next), int64(len(next)), nil, current.Revision); err != nil {
		return errors.New("large stream overwrite")
	}
	staleCtx, _ := client.WithViewOperationID(ctx, "stale-overwrite")
	if err := view.WriteStreamConditional(staleCtx, "/file", bytes.NewReader(data), int64(len(data)), nil, current.Revision); !errors.Is(err, client.ErrConflict) {
		return errors.New("stale overwrite was not rejected")
	}
	if !readEquals(ctx, child, "/file", data) || !readEquals(ctx, view, "/file", next) || !readEquals(ctx, c, "/file", seed) {
		return errors.New("derived view overwrite isolation")
	}
	plan, err := view.InitiateFixedExtentUpload(ctx, "cancel-init", "/cancelled", 6, 0, "")
	if err != nil {
		return errors.New("explicit initiation")
	}
	replay, err := view.InitiateFixedExtentUpload(ctx, "cancel-init", "/cancelled", 6, 0, "")
	if err != nil || replay.UploadID != plan.UploadID || replay.ExpiresAt != plan.ExpiresAt {
		return errors.New("initiation replay")
	}
	aborted, err := view.AbortFixedUpload(ctx, "cancel-upload", plan.UploadID)
	if err != nil || aborted.Status != "ABORTED" {
		return errors.New("explicit cancellation")
	}
	sealed, err := c.SealFixedView(ctx, work, "seal-sdk")
	if err != nil || sealed.State != "DONE" || sealed.Layer.Phase != "READY" {
		return errors.New("seal uploaded view")
	}
	validation, err := c.BeginViewWork(ctx, work.LayerID, "validate-sdk", time.Hour, true)
	if err != nil || !validation.Launch {
		return errors.New("begin validation")
	}
	validator, err := c.WithView(validation.Work.Binding())
	if err != nil || !readEquals(ctx, validator, "/file", next) {
		return errors.New("validation read")
	}
	return nil
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := run(ctx, os.Getenv("DRIVE9_FIXED_SDK_BASE")); err != nil {
		// run returns only fixed stage labels, never server bodies or signed URLs.
		fmt.Fprintln(os.Stderr, "fixed sdk probe failed:", err)
		os.Exit(1)
	}
	fmt.Println("fixed sdk upload probe passed")
}
