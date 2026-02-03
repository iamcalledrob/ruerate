package ruerate

import (
	"math/rand/v2"
	"time"
)

// WithJitter is a convenience function to use with wait times returned from limiter Allow methods.
// In the real world, jitter should almost certainly be applied.
func WithJitter(wait time.Duration, amount float64) time.Duration {
	return wait + time.Duration(float64(wait)*rand.Float64()*amount)
}
