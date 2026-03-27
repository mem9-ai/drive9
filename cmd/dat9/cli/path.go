package cli

import "strings"

type RemotePath struct {
	Context string
	Path    string
}

func ParseRemote(s string) (RemotePath, bool) {
	if s == "-" {
		return RemotePath{}, false
	}
	idx := strings.Index(s, ":/")
	if idx < 0 {
		if strings.HasPrefix(s, ":") {
			return RemotePath{Path: s[1:]}, true
		}
		return RemotePath{}, false
	}
	if idx == 1 {
		return RemotePath{}, false
	}
	return RemotePath{
		Context: s[:idx],
		Path:    s[idx+1:],
	}, true
}
