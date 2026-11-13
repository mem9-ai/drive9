package main

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

// runMerge is invoked only by the paired server's isolated merge fixture.
// The fixture drops the first successful merge response after committing it.
func runMerge(ctx context.Context, base string) error {
	if !localEndpoint(base) {
		return errors.New("invalid loopback endpoint")
	}
	c := client.New(base, "")
	seed := []byte("original main")
	if err := c.WriteCtx(ctx, "/file", seed); err != nil {
		return errors.New("merge seed")
	}
	_, observer, err := createWork(ctx, c, "merge-observer", "")
	if err != nil {
		return err
	}
	work, view, err := createWork(ctx, c, "merge-candidate", "")
	if err != nil {
		return err
	}
	before, err := view.StatCtx(ctx, "/file")
	if err != nil {
		return errors.New("merge inherited stat")
	}
	data := bytes.Repeat([]byte("a"), (4<<20)+19)
	copy(data[(4<<20)-4:], []byte("boundary"))
	writeCtx, err := client.WithViewOperationID(ctx, "merge-upload")
	if err != nil {
		return errors.New("merge upload identity")
	}
	if err := view.WriteStreamConditional(writeCtx, "/file", bytes.NewReader(data), int64(len(data)), nil, before.Revision); err != nil {
		return errors.New("merge extent upload")
	}
	if err := c.WriteCtx(ctx, "/unrelated", []byte("concurrent")); err != nil {
		return errors.New("merge unrelated write")
	}
	sealed, err := c.SealFixedView(ctx, work, "merge-seal")
	if err != nil || sealed.State != "DONE" {
		return errors.New("merge seal")
	}
	validation, err := c.BeginViewWork(ctx, work.LayerID, "merge-validate", time.Hour, true)
	if err != nil || !validation.Launch {
		return errors.New("merge validation")
	}
	validator, err := c.WithView(validation.Work.Binding())
	if err != nil || !readEquals(ctx, validator, "/file", data) {
		return errors.New("merge validation bytes")
	}
	out, err := c.MergeFixedView(ctx, validation.Work, "merge-original")
	var identified *client.ViewOperationError
	if !errors.As(err, &identified) || identified.OperationID != "merge-original" || identified.LayerID != work.LayerID || out != (client.FixedViewOutcome{}) {
		return errors.New("merge response loss identity")
	}
	first, err := c.MergeFixedView(ctx, validation.Work, "merge-original")
	if err != nil || first.State != "DONE" || first.Layer.Phase != "MERGED" {
		return errors.New("merge recovery")
	}
	if !readEquals(ctx, c, "/file", data) || !readEquals(ctx, observer, "/file", seed) || !readEquals(ctx, c, "/unrelated", []byte("concurrent")) {
		return errors.New("merge published isolation")
	}
	if err := overwriteMergedMain(ctx, c); err != nil {
		return errors.New("merge newer write")
	}
	second, err := c.MergeFixedView(ctx, validation.Work, "merge-original")
	if err != nil || second != first || !readEquals(ctx, c, "/file", []byte("newer main")) {
		return errors.New("merge replay changed main")
	}
	return nil
}

// The merged candidate has extent layout even when its next body is small.
func overwriteMergedMain(ctx context.Context, c *client.Client) error {
	current, err := c.StatCtx(ctx, "/file")
	if err != nil || current.Revision < 1 || current.ContentLayout != client.ContentLayoutExtent {
		return errors.New("merge current extent stat")
	}
	data := []byte("newer main")
	return c.WriteMultipartStreamConditional(ctx, "/file", bytes.NewReader(data), int64(len(data)), nil, current.Revision)
}
