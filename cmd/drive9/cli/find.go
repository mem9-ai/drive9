package cli

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func Find(c *client.Client, args []string) error {
	authLocal, args, err := peelObjectAuth(args)
	if err != nil {
		return err
	}
	defer withObjectAuthLocal(authLocal)()
	path := "/"
	params := url.Values{}
	explicitLayer := false

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--layer":
			if i+1 >= len(args) {
				return fmt.Errorf("--layer requires argument")
			}
			i++
			explicitLayer = true
		case "-name":
			if i+1 >= len(args) {
				return fmt.Errorf("-name requires argument")
			}
			i++
			params.Set("name", args[i])
		case "-tag":
			if i+1 >= len(args) {
				return fmt.Errorf("-tag requires key=value argument")
			}
			i++
			params.Set("tag", args[i])
		case "-newer":
			if i+1 >= len(args) {
				return fmt.Errorf("-newer requires date (YYYY-MM-DD)")
			}
			i++
			params.Set("newer", args[i])
		case "-older":
			if i+1 >= len(args) {
				return fmt.Errorf("-older requires date (YYYY-MM-DD)")
			}
			i++
			params.Set("older", args[i])
		case "-size":
			if i+1 >= len(args) {
				return fmt.Errorf("-size requires argument (+N or -N)")
			}
			i++
			v := args[i]
			if strings.HasPrefix(v, "+") {
				params.Set("minsize", v[1:])
			} else if strings.HasPrefix(v, "-") {
				params.Set("maxsize", v[1:])
			} else {
				params.Set("minsize", v)
				params.Set("maxsize", v)
			}
		default:
			if strings.HasPrefix(args[i], "--layer=") {
				explicitLayer = true
			} else if !strings.HasPrefix(args[i], "-") {
				path = args[i]
			} else {
				return fmt.Errorf("unknown flag %q", args[i])
			}
		}
	}

	if explicitLayer || (c != nil && c.ViewBinding().LayerID != "") {
		return fmt.Errorf("find: %w (--layer is not supported on search endpoints)", viewprotocol.ErrUnsupportedInLayerView)
	}
	h, err := fsHandleForArg(c, path)
	if err != nil {
		return err
	}
	if err := requireCapOnHandle(h, CapSearch, "find"); err != nil {
		return err
	}
	c, path = h.Client, h.Path

	results, err := c.Find(path, params)
	if err != nil {
		return err
	}
	for _, r := range results {
		_, _ = fmt.Fprintln(os.Stdout, r.Path)
	}
	return nil
}
