// Package gitwsindex is the shared wire type for /.drive9/git-workspaces/index.json.
// Client and server both mutate this document; unknown keys must survive a rewrite.
package gitwsindex

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// ErrUnreadable is returned when the index document is malformed or exceeds
// the size/entry limits. Mounts treat this as a permanent dormant signal.
var ErrUnreadable = errors.New("git workspace index is unreadable")

const (
	Path       = "/.drive9/git-workspaces/index.json"
	MaxBytes   = 1 << 20
	MaxEntries = 4096
)

// Index is the remote existence document. Extra top-level keys are preserved
// across Marshal so a writer that does not know a newer field cannot strip it.
type Index struct {
	Version    int
	UpdatedAt  string
	Workspaces []Entry
	extra      map[string]json.RawMessage
}

// Entry is one workspace existence record. Extra per-entry keys are preserved.
type Entry struct {
	WorkspaceID   string
	RootPath      string
	WorkspaceKind string
	extra         map[string]json.RawMessage
}

func (idx *Index) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	extra := make(map[string]json.RawMessage)
	for k, v := range raw {
		switch k {
		case "version":
			if err := json.Unmarshal(v, &idx.Version); err != nil {
				return fmt.Errorf("git workspace index version: %w", err)
			}
		case "updated_at":
			if err := json.Unmarshal(v, &idx.UpdatedAt); err != nil {
				return fmt.Errorf("git workspace index updated_at: %w", err)
			}
		case "workspaces":
			if err := json.Unmarshal(v, &idx.Workspaces); err != nil {
				return fmt.Errorf("git workspace index workspaces: %w", err)
			}
		default:
			extra[k] = v
		}
	}
	if len(extra) > 0 {
		idx.extra = extra
	}
	return nil
}

func (idx Index) MarshalJSON() ([]byte, error) {
	raw := make(map[string]json.RawMessage, 3+len(idx.extra))
	for k, v := range idx.extra {
		raw[k] = v
	}
	var err error
	if raw["version"], err = json.Marshal(idx.Version); err != nil {
		return nil, err
	}
	if raw["updated_at"], err = json.Marshal(idx.UpdatedAt); err != nil {
		return nil, err
	}
	if raw["workspaces"], err = json.Marshal(idx.Workspaces); err != nil {
		return nil, err
	}
	return json.Marshal(raw)
}

func (e *Entry) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	extra := make(map[string]json.RawMessage)
	for k, v := range raw {
		switch k {
		case "workspace_id":
			if err := json.Unmarshal(v, &e.WorkspaceID); err != nil {
				return fmt.Errorf("workspace_id: %w", err)
			}
		case "root_path":
			if err := json.Unmarshal(v, &e.RootPath); err != nil {
				return fmt.Errorf("root_path: %w", err)
			}
		case "workspace_kind":
			if err := json.Unmarshal(v, &e.WorkspaceKind); err != nil {
				return fmt.Errorf("workspace_kind: %w", err)
			}
		default:
			extra[k] = v
		}
	}
	if len(extra) > 0 {
		e.extra = extra
	}
	return nil
}

func (e Entry) MarshalJSON() ([]byte, error) {
	raw := make(map[string]json.RawMessage, 3+len(e.extra))
	for k, v := range e.extra {
		raw[k] = v
	}
	var err error
	if raw["workspace_id"], err = json.Marshal(e.WorkspaceID); err != nil {
		return nil, err
	}
	if raw["root_path"], err = json.Marshal(e.RootPath); err != nil {
		return nil, err
	}
	if e.WorkspaceKind != "" {
		if raw["workspace_kind"], err = json.Marshal(e.WorkspaceKind); err != nil {
			return nil, err
		}
	}
	return json.Marshal(raw)
}

// Parse decodes an index document. Empty input is a valid empty index.
func Parse(data []byte) (*Index, error) {
	if int64(len(data)) > MaxBytes {
		return nil, fmt.Errorf("%w: exceeds maximum size (%d bytes)", ErrUnreadable, MaxBytes)
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return &Index{Version: 1}, nil
	}
	var idx Index
	if err := json.Unmarshal(data, &idx); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnreadable, err)
	}
	if len(idx.Workspaces) > MaxEntries {
		return nil, fmt.Errorf("%w: exceeds maximum entry count (%d)", ErrUnreadable, MaxEntries)
	}
	return &idx, nil
}
