package cli

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/mem9-ai/dat9/pkg/client"
)

func Find(c *client.Client, args []string) error {
	if IsFindHelpArgs(args) {
		_, _ = fmt.Fprintln(os.Stdout, fsFindUsage())
		return nil
	}
	path := "/"
	params := url.Values{}
	nextValue := func(i *int, flag, want string) (string, error) {
		if *i+1 >= len(args) {
			return "", fmt.Errorf("%s requires %s", flag, want)
		}
		(*i)++
		if args[*i] == "--" {
			if *i+1 >= len(args) {
				return "", fmt.Errorf("%s requires %s", flag, want)
			}
			(*i)++
		}
		return args[*i], nil
	}

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-name":
			v, err := nextValue(&i, "-name", "argument")
			if err != nil {
				return err
			}
			params.Set("name", v)
		case "-tag":
			v, err := nextValue(&i, "-tag", "key=value argument")
			if err != nil {
				return err
			}
			params.Set("tag", v)
		case "-newer":
			v, err := nextValue(&i, "-newer", "date (YYYY-MM-DD)")
			if err != nil {
				return err
			}
			params.Set("newer", v)
		case "-older":
			v, err := nextValue(&i, "-older", "date (YYYY-MM-DD)")
			if err != nil {
				return err
			}
			params.Set("older", v)
		case "-size":
			v, err := nextValue(&i, "-size", "argument (+N or -N)")
			if err != nil {
				return err
			}
			if strings.HasPrefix(v, "+") {
				params.Set("minsize", v[1:])
			} else if strings.HasPrefix(v, "-") {
				params.Set("maxsize", v[1:])
			} else {
				params.Set("minsize", v)
				params.Set("maxsize", v)
			}
		default:
			if !strings.HasPrefix(args[i], "-") {
				path = args[i]
			} else {
				return fmt.Errorf("unknown flag %q", args[i])
			}
		}
	}

	var err error
	c, path, _, _, err = fsClientForRemoteArg(c, path)
	if err != nil {
		return err
	}

	results, err := c.Find(path, params)
	if err != nil {
		return err
	}
	for _, r := range results {
		_, _ = fmt.Fprintln(os.Stdout, r.Path)
	}
	return nil
}
