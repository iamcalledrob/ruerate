package safetime

import (
	"math"
	"time"
)

var (
	MaxTime = time.Unix(1<<63-62135596801, 999999999)
	MinTime = time.Unix(-62135596800, 0)
)

// Add returns time+d, with bounds checks to prevent wraparound
// Preserves monotonic component unless truncated
func Add(t time.Time, d time.Duration) time.Time {
	if d > 0 && t.After(MaxTime.Add(-d)) {
		return MaxTime

	} else if d < 0 && t.Before(MinTime.Add(-d)) {
		return MinTime

	}
	return t.Add(d)
}

// Duration casts a float64 nanos value to a time.Duration,
// with bounds checks to prevent Int64 overflows
func Duration(nanos float64) time.Duration {
	// Use 1 << 63 instead of float64(math.MaxInt64) (aka 1 << 63 - 1) because the
	// float64(math.MaxInt64) cast can *theoretically* round to an overflowing value.
	const limit = 1 << 63
	if math.IsNaN(nanos) {
		return 0
	}
	if nanos >= limit {
		return time.Duration(math.MaxInt64)
	}
	if nanos <= -limit {
		return time.Duration(math.MinInt64)
	}

	return time.Duration(nanos)
}
