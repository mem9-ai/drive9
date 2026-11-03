package writebuffer

import (
	"fmt"
	"log"
	"strings"
)

// safeLogPrintf escapes control whitespace in strings and errors before
// logging (log-injection discipline; same behavior as staging's helper —
// the leaf package keeps its own copy rather than importing the ledger).
func safeLogPrintf(format string, args ...any) {
	for i, arg := range args {
		switch arg.(type) {
		case string:
			args[i] = safeLogValue{value: arg}
		case error:
			args[i] = safeLogValue{value: arg}
		}
	}
	log.Printf(format, args...)
}

type safeLogValue struct{ value any }

func (v safeLogValue) String() string {
	switch t := v.value.(type) {
	case string:
		return strings.NewReplacer("\n", "\\n", "\r", "\\r", "\t", "\\t").Replace(t)
	case error:
		return strings.NewReplacer("\n", "\\n", "\r", "\\r", "\t", "\\t").Replace(t.Error())
	}
	return fmt.Sprint(v.value)
}
