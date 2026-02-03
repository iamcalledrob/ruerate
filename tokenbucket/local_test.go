package tokenbucket

import (
	"testing"
	"time"

	ruerate "github.com/iamcalledrob/ruebucket"
	"github.com/stretchr/testify/require"
)

func TestLocalLimiter(t *testing.T) {
	// Local limiter is always replenishable
	testLimiter_Common(t, func(opts LimiterOpts) (Limiter, error) {
		return NewLocalLimiter(&opts)
	})
}

func TestLocalKeyedLimiter(t *testing.T) {
	testKeyedLimiter_Common(t, func(opts LimiterOpts) (CacheableKeyedLimiter, error) {
		return NewLocalKeyedLimiter(opts)
	})

	// Ensures cache keys have an accurate TTL set, so they don't linger in ttlcache longer than necessary.
	t.Run("TTL", func(t *testing.T) {
		// capacity = 50, refills at 25/sec
		// -> 2 seconds to completely fill bucket from empty
		lim, err := NewLocalKeyedLimiter(LimiterOpts{
			RatePerSec: ruerate.Every(40 * time.Millisecond),
			Capacity:   50,
		})
		require.NoError(t, err)

		// Take 7 tokens, leaving 43 tokens in the bucket
		ok, wait, err := lim.AllowN(t.Context(), "key", 7)
		require.NoError(t, err)
		require.Zero(t, wait)
		require.True(t, ok)

		// Check TTL.
		// Refilling at 25/sec, the 7 tokens we took should take 0.28s (7/25) to replenish
		// Therefore TTL should be 0.28, as expired keys = full bucket

		// Allow 2ms slop for slow execution
		entry := lim.limiters.Get("key")
		msec := entry.TTL().Milliseconds()
		if msec < 278 || msec > 282 {
			t.Fatalf("TTL out of bounds: ms = %d", msec)
		}
	})
}
