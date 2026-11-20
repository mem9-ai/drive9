// Package viewprotocol defines the filesystem view binding shared by HTTP tools
// and mounts. A binding identifies work; authentication remains server-owned.
package viewprotocol

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
)

const (
	ViewHeader       = "X-Drive9-View"
	WorkHeader       = "X-Drive9-Work-Id"
	FenceHeader      = "X-Drive9-Work-Fence"
	ValidationHeader = "X-Drive9-Validation"
)

var ErrBinding = errors.New("invalid filesystem view binding")

// Binding's zero value means main. A Layer request always identifies its work
// generation; no supplied lease timestamp can extend server-side eligibility.
type Binding struct {
	LayerID    string
	WorkID     string
	Fence      int64
	Validation bool
}

func validID(id string) bool {
	if len(id) < 1 || len(id) > 64 {
		return false
	}
	for i := range id {
		c := id[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-', c == '_':
		default:
			return false
		}
	}
	return true
}

func (b Binding) Validate() error {
	if b.LayerID == "" {
		if b.WorkID != "" || b.Fence != 0 || b.Validation {
			return ErrBinding
		}
		return nil
	}
	if !validID(b.LayerID) || strings.EqualFold(b.LayerID, "main") || !validID(b.WorkID) || b.Fence < 1 {
		return ErrBinding
	}
	return nil
}

// Parse rejects partial and repeated bindings instead of choosing main or the
// first header value. Explicit main and omitted headers have identical meaning.
func Parse(h http.Header) (Binding, error) {
	var values [4]string
	for i, key := range []string{ViewHeader, WorkHeader, FenceHeader, ValidationHeader} {
		rows := h.Values(key)
		if len(rows) > 1 || len(rows) == 1 && rows[0] == "" {
			return Binding{}, ErrBinding
		}
		if len(rows) == 1 {
			values[i] = rows[0]
		}
	}
	view, work, fence, validation := values[0], values[1], values[2], values[3]
	if view == "" || view == "main" {
		if work != "" || fence != "" || validation != "" {
			return Binding{}, ErrBinding
		}
		return Binding{}, nil
	}
	n, err := strconv.ParseInt(fence, 10, 64)
	if err != nil || strconv.FormatInt(n, 10) != fence {
		return Binding{}, ErrBinding
	}
	if validation != "" && validation != "true" && validation != "false" {
		return Binding{}, ErrBinding
	}
	b := Binding{LayerID: view, WorkID: work, Fence: n, Validation: validation == "true"}
	if err := b.Validate(); err != nil {
		return Binding{}, err
	}
	return b, nil
}

// Apply replaces all four binding headers, clearing stale Layer metadata when
// binding to main. The caller must use a request it owns.
func (b Binding) Apply(h http.Header) error {
	if err := b.Validate(); err != nil {
		return err
	}
	for _, key := range []string{ViewHeader, WorkHeader, FenceHeader, ValidationHeader} {
		h.Del(key)
	}
	if b.LayerID == "" {
		return nil
	}
	h.Set(ViewHeader, b.LayerID)
	h.Set(WorkHeader, b.WorkID)
	h.Set(FenceHeader, strconv.FormatInt(b.Fence, 10))
	h.Set(ValidationHeader, strconv.FormatBool(b.Validation))
	return nil
}
