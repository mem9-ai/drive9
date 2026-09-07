package fuse

import (
	"path"
	"strings"
)

func (fh *FileHandle) isExtent() bool {
	return fh != nil && fh.extentW != nil
}

func (fs *Dat9FS) shouldUseExtentPath(localPath string) bool {
	if fs == nil || fs.opts == nil {
		return false
	}
	for _, pat := range fs.opts.ExtentPaths {
		if matchExtentPattern(pat, localPath) {
			return true
		}
	}
	return false
}

func matchExtentPattern(pattern, filePath string) bool {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return false
	}
	name := path.Base(filePath)
	if pattern == "*" {
		return true
	}
	if strings.HasPrefix(pattern, "*.") {
		return strings.HasSuffix(strings.ToLower(name), strings.ToLower(pattern[1:]))
	}
	if strings.Contains(pattern, "*") {
		ok, _ := path.Match(path.Base(pattern), name)
		return ok
	}
	return strings.EqualFold(name, pattern)
}
