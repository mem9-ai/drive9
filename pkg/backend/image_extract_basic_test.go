package backend

import (
	"context"
	"errors"
	"testing"
)

type countingImageExtractor struct {
	text  string
	err   error
	calls int
}

func (e *countingImageExtractor) ExtractImageText(ctx context.Context, req ImageExtractRequest) (string, ImageExtractUsage, error) {
	e.calls++
	return e.text, ImageExtractUsage{}, e.err
}

func TestFallbackImageTextExtractorPropagatesPrimaryError(t *testing.T) {
	primaryErr := errors.New("vl api: 400 unsupported image format")
	primary := &countingImageExtractor{err: primaryErr}
	fallback := &countingImageExtractor{text: "basic text"}
	e := NewFallbackImageTextExtractor(primary, fallback)

	_, _, err := e.ExtractImageText(context.Background(), ImageExtractRequest{FileID: "f1", Path: "/img/a.png"})
	if !errors.Is(err, primaryErr) {
		t.Fatalf("expected primary error to propagate, got %v", err)
	}
	if fallback.calls != 0 {
		t.Fatalf("fallback must not run when primary errors, got %d calls", fallback.calls)
	}
}

func TestFallbackImageTextExtractorUsesPrimaryText(t *testing.T) {
	primary := &countingImageExtractor{text: "rich caption"}
	fallback := &countingImageExtractor{text: "basic text"}
	e := NewFallbackImageTextExtractor(primary, fallback)

	text, _, err := e.ExtractImageText(context.Background(), ImageExtractRequest{FileID: "f1", Path: "/img/a.png"})
	if err != nil {
		t.Fatal(err)
	}
	if text != "rich caption" {
		t.Fatalf("got %q", text)
	}
	if fallback.calls != 0 {
		t.Fatalf("fallback must not run on primary success, got %d calls", fallback.calls)
	}
}

func TestFallbackImageTextExtractorFallsBackOnEmptyPrimaryText(t *testing.T) {
	primary := &countingImageExtractor{text: "  \n"}
	fallback := &countingImageExtractor{text: "basic text"}
	e := NewFallbackImageTextExtractor(primary, fallback)

	text, _, err := e.ExtractImageText(context.Background(), ImageExtractRequest{FileID: "f1", Path: "/img/a.png"})
	if err != nil {
		t.Fatal(err)
	}
	if text != "basic text" {
		t.Fatalf("got %q", text)
	}
	if primary.calls != 1 || fallback.calls != 1 {
		t.Fatalf("calls primary=%d fallback=%d", primary.calls, fallback.calls)
	}
}
