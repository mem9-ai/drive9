package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/mem9-ai/dat9/pkg/tenant/schemadump"
)

func runSchemaCommand(args []string) error {
	if len(args) == 0 {
		return errors.New(schemadump.Usage())
	}

	switch args[0] {
	case "dump-init-sql":
		return dumpInitSQL(args[1:])
	default:
		return fmt.Errorf("unknown schema subcommand %q\n%s", args[0], schemadump.Usage())
	}
}

func dumpInitSQL(args []string) error {
	provider := ""

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--provider":
			if i+1 >= len(args) {
				return fmt.Errorf("--provider requires an argument")
			}
			i++
			provider = args[i]
		default:
			return fmt.Errorf("unknown flag %q\n%s", args[i], schemadump.Usage())
		}
	}

	resolved, err := schemadump.ResolveProvider(provider)
	if err != nil {
		return err
	}
	sqlText, err := schemadump.SQLText(resolved)
	if err != nil {
		return err
	}
	_, err = fmt.Fprint(os.Stdout, sqlText)
	return err
}
