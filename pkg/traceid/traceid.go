// Package traceid manages trace IDs in contexts.
package traceid

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"time"
)

type keyType int

const key keyType = iota

func FromContext(ctx context.Context) string {
	v, _ := ctx.Value(key).(string)
	return v
}

func With(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, key, traceID)
}

func Generate() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(b)
}

func Background() context.Context {
	return With(context.Background(), Generate())
}
