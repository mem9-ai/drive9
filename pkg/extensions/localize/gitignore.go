package localize

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/pathutil"
)

// ignoreRule is one parsed ignore-file line.
type ignoreRule struct {
	negated   bool
	dirOnly   bool
	anchored  bool
	anyPrefix bool
	segRe     string
}

// ignoreLayer is the rule set of one directory's ignore file (or the
// repository's info/exclude).
type ignoreLayer struct {
	dir      string
	anchored []ignoreRule
	anyDepth []ignoreRule
}

// ignoreRuleset is the stacked rule chain from the repository root down to
// the queried directory, cached under a fingerprint of every source body.
type ignoreRuleset struct {
	layers []ignoreLayer
	fp     string
}

// gitIgnoreCheck reports whether the repository's ignore rules cover
// viewPath, and whether a repository could be consulted at all. It reads
// the ignore files through the inner view (the chain below this handler):
// each directory from the repository root down contributes its rules, with
// deeper files taking precedence, and the repository's info/exclude
// underneath them all. A path with no rule chain (no repository above it,
// or the repository root itself) is unverified.
func (f *Localize) gitIgnoreCheck(ctx context.Context, inner fs.Filesystem, viewPath string, dirHint bool) (confirmed, verified bool) {
	dir := viewPath
	if !dirHint && !strings.HasSuffix(viewPath, "/") {
		dir = pathutil.DirOf(viewPath)
	}
	root, ok := f.repoRoot(ctx, inner, dir)
	if !ok {
		return false, false
	}
	set := f.loadIgnoreRules(ctx, inner, root, dir)
	if set == nil {
		return false, false
	}
	rel := strings.TrimPrefix(strings.TrimPrefix(viewPath, root), "/")
	if rel == "" {
		return false, true
	}
	decision := false
	for _, layer := range set.layers {
		lrel := strings.TrimPrefix(strings.TrimPrefix(layer.dir, root), "/")
		lrel = strings.TrimPrefix(lrel, "/")
		layerRel := rel
		if lrel != "" {
			layerRel = strings.TrimPrefix(rel, lrel+"/")
			if layerRel == rel {
				continue
			}
		}
		isDir := dirHint || strings.HasSuffix(viewPath, "/")
		for _, r := range layer.anchored {
			if r.matches(layerRel, isDir) {
				decision = !r.negated
			}
		}
		for _, r := range layer.anyDepth {
			if r.matches(layerRel, isDir) {
				decision = !r.negated
			}
		}
	}
	return decision, true
}

// loadIgnoreRules builds (and caches by source fingerprint) the rule chain
// for dir: the repository's info/exclude plus every directory's ignore file
// from the repository root down to dir.
func (f *Localize) loadIgnoreRules(ctx context.Context, inner fs.Filesystem, root, dir string) *ignoreRuleset {
	var dirs []string
	for cur := dir; ; cur = pathutil.DirOf(cur) {
		dirs = append(dirs, cur)
		if cur == root || cur == "/" || cur == "" {
			break
		}
	}
	for i, j := 0, len(dirs)-1; i < j; i, j = i+1, j-1 {
		dirs[i], dirs[j] = dirs[j], dirs[i]
	}
	var sources []string
	bodies := make([]string, 0, len(dirs)+1)
	if body, ok := f.readInnerFile(ctx, inner, joinDir(joinDir(root, ".git"), "info/exclude")); ok {
		sources = append(sources, "exclude:"+body)
		bodies = append(bodies, body)
	} else {
		bodies = append(bodies, "")
	}
	for _, d := range dirs {
		body, ok := f.readInnerFile(ctx, inner, joinDir(d, ".gitignore"))
		if !ok {
			body = ""
		}
		sources = append(sources, d+":"+body)
		bodies = append(bodies, body)
	}
	fp := fmt.Sprintf("%s|%v", root, sources)
	f.innerMu.Lock()
	cached, ok := f.ignoreSets[fp]
	f.innerMu.Unlock()
	if ok {
		return cached
	}
	set := &ignoreRuleset{fp: fp}
	set.layers = append(set.layers, parseIgnoreRules(root, bodies[0]))
	for i, d := range dirs {
		if bodies[i+1] != "" || d == dir {
			set.layers = append(set.layers, parseIgnoreRules(d, bodies[i+1]))
		}
	}
	f.innerMu.Lock()
	f.ignoreSets[fp] = set
	f.innerMu.Unlock()
	return set
}

// readInnerFile reads a whole file through the inner view. Missing or
// unreadable files report ok=false.
func (f *Localize) readInnerFile(ctx context.Context, inner fs.Filesystem, tenantPath string) (string, bool) {
	if inner == nil {
		return "", false
	}
	data, err := inner.Read(ctx, tenantPath, 0, 0)
	if err != nil {
		return "", false
	}
	return string(data), true
}

// repoRoot finds the nearest ancestor of dir that contains a .git entry,
// cached per queried directory (negative answers cache too).
func (f *Localize) repoRoot(ctx context.Context, inner fs.Filesystem, dir string) (string, bool) {
	f.innerMu.Lock()
	root, ok := f.repoRoots[dir]
	f.innerMu.Unlock()
	if ok {
		return root, root != ""
	}
	if inner == nil {
		return "", false
	}
	cur := dir
	for {
		if _, err := inner.GetAttr(ctx, joinDir(cur, ".git")); err == nil {
			f.innerMu.Lock()
			f.repoRoots[dir] = cur
			f.innerMu.Unlock()
			return cur, true
		}
		if cur == "/" || cur == "" {
			break
		}
		cur = pathutil.DirOf(cur)
	}
	f.innerMu.Lock()
	f.repoRoots[dir] = ""
	f.innerMu.Unlock()
	return "", false
}

func (r ignoreRule) matches(rel string, isDir bool) bool {
	if r.dirOnly && !isDir {
		return false
	}
	if r.anchored {
		return regexpMatchString(`^`+r.segRe+`$`, rel) || regexpMatchString(`^`+r.segRe+`(/.*)?$`, rel)
	}
	if r.anyPrefix {
		return regexpMatchString(`(^|.*/)`+r.segRe+`$`, rel)
	}
	base := rel
	if i := strings.LastIndexByte(rel, '/'); i >= 0 {
		base = rel[i+1:]
	}
	return regexpMatchString(`^`+r.segRe+`$`, base)
}

// ignoreRuleRegex translates one ignore pattern body into a regular
// expression over path segments: ** spans separators, * and ? stay within
// one segment, and everything else is literal.
func ignoreRuleRegex(body string) string {
	var b strings.Builder
	for i := 0; i < len(body); i++ {
		switch c := body[i]; c {
		case '*':
			if i+1 < len(body) && body[i+1] == '*' {
				b.WriteString(`.*`)
				i++
			} else {
				b.WriteString(`[^/]*`)
			}
		case '?':
			b.WriteString(`[^/]`)
		case '.', '(', ')', '+', '|', '^', '$', '[', ']', '{', '}', '\\':
			b.WriteByte('\\')
			b.WriteByte(c)
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

// joinDir appends name to an absolute slash directory path.
func joinDir(dir, name string) string {
	if dir == "/" || dir == "" {
		return "/" + name
	}
	return dir + "/" + name
}

// parseIgnoreRules parses one ignore-file body into a layer of rules.
func parseIgnoreRules(dir, body string) ignoreLayer {
	layer := ignoreLayer{dir: dir}
	for _, raw := range strings.Split(body, "\n") {
		line := strings.TrimRight(raw, "\r")
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		r := ignoreRule{}
		if strings.HasPrefix(line, "!") {
			r.negated = true
			line = line[1:]
		}
		if line == "" {
			continue
		}
		if strings.HasSuffix(line, "/") {
			r.dirOnly = true
			line = strings.TrimSuffix(line, "/")
		}
		if line == "" {
			continue
		}
		body := line
		if strings.HasPrefix(body, "**/") {
			body = body[3:]
			r.anyPrefix = true
		} else if strings.Contains(body, "/") {
			r.anchored = true
			body = strings.TrimPrefix(body, "/")
		}
		r.segRe = ignoreRuleRegex(body)
		if r.anchored {
			layer.anchored = append(layer.anchored, r)
		} else {
			layer.anyDepth = append(layer.anyDepth, r)
		}
	}
	return layer
}

// regexpMatchString reports a compiled match; an invalid pattern simply
// never matches.
func regexpMatchString(pattern, s string) bool {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return false
	}
	return re.MatchString(s)
}
