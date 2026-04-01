// Package embedding provides text embedding clients and vector helpers.
package embedding

import "context"

// Client embeds query or document text into a dense float32 vector.
type Client interface {
	// EmbedText returns a dense vector for the supplied text. It returns
	// (nil, nil) when embeddings are intentionally disabled.
	EmbedText(ctx context.Context, text string) ([]float32, error)
}

// NopClient disables embeddings while keeping caller fallback paths simple.
type NopClient struct{}

// EmbedText implements Client.
func (NopClient) EmbedText(context.Context, string) ([]float32, error) { return nil, nil }
