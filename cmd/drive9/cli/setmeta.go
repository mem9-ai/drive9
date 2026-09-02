package cli

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/mem9-ai/drive9/pkg/backend"
	"github.com/mem9-ai/drive9/pkg/client"
)

// SetMeta updates tags and/or description of an existing remote file without
// rewriting its content.
func SetMeta(c *client.Client, args []string) error {
	authLocal, args, err := peelObjectAuth(args)
	if err != nil {
		return err
	}
	defer withObjectAuthLocal(authLocal)()

	var tags map[string]string
	tagsSet := false
	clearTags := false
	var description string
	descriptionSet := false
	clearDescription := false

	var positional []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--":
			positional = append(positional, args[i+1:]...)
			i = len(args)
		case a == "--tag":
			if i+1 >= len(args) {
				return fmt.Errorf("--tag requires argument")
			}
			i++
			tags, err = parseAndMergeTag(tags, args[i])
			if err != nil {
				return err
			}
			tagsSet = true
		case strings.HasPrefix(a, "--tag="):
			tags, err = parseAndMergeTag(tags, strings.TrimPrefix(a, "--tag="))
			if err != nil {
				return err
			}
			tagsSet = true
		case a == "--clear-tags":
			clearTags = true
		case a == "--description":
			if i+1 >= len(args) {
				return fmt.Errorf("--description requires argument")
			}
			i++
			description = args[i]
			descriptionSet = true
		case strings.HasPrefix(a, "--description="):
			description = strings.TrimPrefix(a, "--description=")
			descriptionSet = true
		case a == "--clear-description":
			clearDescription = true
		case strings.HasPrefix(a, "-"):
			return fmt.Errorf("unknown flag %q", a)
		default:
			positional = append(positional, a)
		}
	}
	if tagsSet && clearTags {
		return fmt.Errorf("--tag and --clear-tags are mutually exclusive")
	}
	if descriptionSet && clearDescription {
		return fmt.Errorf("--description and --clear-description are mutually exclusive")
	}
	if !tagsSet && !clearTags && !descriptionSet && !clearDescription {
		return fmt.Errorf("usage: drive9 fs setmeta [--tag key=value]... [--clear-tags] [--description <text>] [--clear-description] <path>")
	}
	if len(positional) != 1 {
		return fmt.Errorf("usage: drive9 fs setmeta [--tag key=value]... [--clear-tags] [--description <text>] [--clear-description] <path>")
	}
	if descriptionSet && utf8.RuneCountInString(description) > backend.MaxDescriptionLen {
		return fmt.Errorf("description exceeds %d characters", backend.MaxDescriptionLen)
	}

	h, err := fsHandleForArg(c, positional[0])
	if err != nil {
		return err
	}
	if err := requireCapOnHandle(h, CapWrite, "setmeta"); err != nil {
		return err
	}
	c, path := h.Client, h.Path

	var opts client.SetMetadataOptions
	if tagsSet || clearTags {
		if tags == nil {
			tags = map[string]string{}
		}
		opts.Tags = tags
	}
	if descriptionSet || clearDescription {
		opts.Description = &description
	}
	return c.SetMetadata(path, opts)
}
