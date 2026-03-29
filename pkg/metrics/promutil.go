package metrics

import (
	"sort"
	"strconv"
	"strings"
)

// SortedKeys returns sorted keys for stable Prometheus output.
func SortedKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func CloneIntMap(src map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

func CloneFloatMap(src map[string]float64) map[string]float64 {
	out := make(map[string]float64, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

func CloneBucketMap(src map[string][]int64) map[string][]int64 {
	out := make(map[string][]int64, len(src))
	for k, v := range src {
		vv := make([]int64, len(v))
		copy(vv, v)
		out[k] = vv
	}
	return out
}

func FormatPromBound(v float64) string {
	return strconv.FormatFloat(v, 'g', -1, 64)
}

func EscapePromLabel(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, `"`, `\\"`)
	s = strings.ReplaceAll(s, "\n", "\\n")
	return s
}
