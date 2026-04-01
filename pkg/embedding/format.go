package embedding

import (
	"math"
	"strconv"
	"strings"
)

// FormatVector formats a float32 slice as a TiDB VECTOR literal string.
// NaN and Inf values are replaced with 0 to avoid invalid vector literals.
func FormatVector(v []float32) string {
	if len(v) == 0 {
		return ""
	}
	var b strings.Builder
	b.Grow(2 + (len(v) - 1) + len(v)*10)
	b.WriteByte('[')
	for i, f := range v {
		if i > 0 {
			b.WriteByte(',')
		}
		f64 := float64(f)
		if math.IsNaN(f64) || math.IsInf(f64, 0) {
			b.WriteByte('0')
			continue
		}
		b.WriteString(strconv.FormatFloat(f64, 'g', -1, 32))
	}
	b.WriteByte(']')
	return b.String()
}
