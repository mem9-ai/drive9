package cli

// IsHelpArg reports whether arg is one of the accepted CLI help spellings.
func IsHelpArg(arg string) bool {
	switch arg {
	case "-h", "-help", "--help", "help":
		return true
	default:
		return false
	}
}
