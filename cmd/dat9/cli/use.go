package cli

import (
	"fmt"
)

func Use(args []string) error {
	if len(args) < 1 {
		cfg := loadConfig()
		if cfg.DefaultDB == "" {
			fmt.Println("no default database set")
		} else {
			fmt.Printf("current default: %s\n", cfg.DefaultDB)
		}
		return nil
	}
	name := args[0]
	cfg := loadConfig()
	if _, ok := cfg.Databases[name]; !ok {
		return fmt.Errorf("database %q not found; run: dat9 db create %s", name, name)
	}
	cfg.SetDefault(name)
	if err := saveConfig(cfg); err != nil {
		return err
	}
	fmt.Printf("default database set to %q\n", name)
	return nil
}
