package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

// ParseLayerViewFlags extracts --layer <ref> and --fence <N> from args,
// returning the remaining arguments in order.
func ParseLayerViewFlags(args []string) (layerRef string, fence int64, rest []string, err error) {
	rest = make([]string, 0, len(args))
	var fenceRaw string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			rest = append(rest, args[i:]...)
			break
		}
		switch {
		case arg == "--layer":
			if i+1 >= len(args) {
				return "", 0, nil, fmt.Errorf("--layer requires argument")
			}
			i++
			layerRef = strings.TrimSpace(args[i])
		case strings.HasPrefix(arg, "--layer="):
			layerRef = strings.TrimSpace(strings.TrimPrefix(arg, "--layer="))
		case arg == "--fence":
			if i+1 >= len(args) {
				return "", 0, nil, fmt.Errorf("--fence requires positive odd integer")
			}
			i++
			fenceRaw = strings.TrimSpace(args[i])
		case strings.HasPrefix(arg, "--fence="):
			fenceRaw = strings.TrimSpace(strings.TrimPrefix(arg, "--fence="))
		default:
			rest = append(rest, arg)
		}
	}
	if fenceRaw != "" {
		parsed, perr := strconv.ParseInt(fenceRaw, 10, 64)
		if perr != nil || parsed <= 0 || parsed%2 == 0 {
			return "", 0, nil, fmt.Errorf("invalid --fence %q (must be a positive odd integer)", fenceRaw)
		}
		fence = parsed
	}
	if fence != 0 && layerRef == "" {
		return "", 0, nil, fmt.Errorf("--fence requires --layer")
	}
	return layerRef, fence, rest, nil
}

// BindCLIViewClient resolves layerRef (if not already a canonical layer_... ID)
// and returns a shallow view-bound Client.
func BindCLIViewClient(ctx context.Context, c *client.Client, layerRef string, fence int64) (*client.Client, error) {
	ref := strings.TrimSpace(layerRef)
	if ref == "" {
		if fence != 0 {
			return nil, fmt.Errorf("--fence requires --layer")
		}
		return c, nil
	}
	if c == nil {
		return nil, fmt.Errorf("missing drive9 client")
	}
	if c.ViewBinding().LayerID != "" {
		return nil, fmt.Errorf("--layer specified multiple times")
	}
	layerID := ref
	if err := viewprotocol.ValidateLayerID(layerID); err != nil {
		info, gerr := c.GetLayer(ctx, ref)
		if gerr != nil {
			return nil, fmt.Errorf("resolve layer %q: %w", ref, gerr)
		}
		layerID = info.LayerID
	}
	return c.WithView(client.ViewBinding{LayerID: layerID, Fence: fence})
}

func requireNoLayerWithRemoteContext(layerRef string, c *client.Client, rp RemotePath, raw string) error {
	activeLayer := strings.TrimSpace(layerRef)
	if activeLayer == "" && c != nil {
		activeLayer = c.ViewBinding().LayerID
	}
	if activeLayer == "" || rp.Context == "" {
		return nil
	}
	return fmt.Errorf("--layer with context-scoped remote path %q is not supported; switch context first or omit the prefix", raw)
}

func parseLayerTTLSeconds(raw string) (int64, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, nil
	}
	if secs, err := strconv.ParseInt(s, 10, 64); err == nil {
		if secs < 0 {
			return 0, fmt.Errorf("invalid --ttl %q (must be >= 0)", raw)
		}
		return secs, nil
	}
	dur, err := time.ParseDuration(s)
	if err != nil || dur <= 0 || dur%time.Second != 0 {
		return 0, fmt.Errorf("invalid --ttl %q (expected whole seconds or duration such as 1h)", raw)
	}
	return int64(dur / time.Second), nil
}

func readAllStdin() ([]byte, error) {
	data, err := io.ReadAll(os.Stdin)
	if err != nil {
		return nil, fmt.Errorf("read stdin: %w", err)
	}
	return data, nil
}
