package tokenbucket

import (
	"math"
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

	// Ensures the "fullAt" value returned by updateLocked is accurate.
	// There have been bugs here before, and they are hard to spot because
	// fullAt is only used for internal cache invalidation.
	t.Run("FullAt", func(t *testing.T) {
		// capacity = 50, refills at 2/sec
		// -> 25 seconds to completely fill bucket from empty
		lim, err := NewLocalLimiter(&LimiterOpts{
			RatePerSec: ruerate.Every(500 * time.Millisecond),
			Capacity:   50,
		})
		require.NoError(t, err)

		// Take 7 tokens, leaving 43 tokens in the bucket
		ok, wait, fullAt, err := lim.updateLocked(-7)
		require.NoError(t, err)
		require.True(t, ok)
		require.Zero(t, wait)

		// Refilling at 2/sec, the 7 tokens we took should take 3.5s to replenish
		// Therefore TTL should be 3.5s, as expired keys = full bucket
		// Allow a little slop (50ms) for test execution time
		require.WithinDuration(t, time.Now().Add(3500*time.Millisecond), fullAt, 5*time.Millisecond)

		// Now test to make sure TTLs accurate reflect token accrual of an *existing* bucket
		// There have been bugs here in the past.

		// Wait enough time for more tokens to replenish
		// 750ms = 1.5 tokens
		<-time.After(750 * time.Millisecond)

		// Take 1 token, theoretically leaving 43 + (1.5) - 1 = 43.5 tokens in the bucket
		ok, wait, fullAt, err = lim.updateLocked(-1)
		require.NoError(t, err)
		require.Zero(t, wait)
		require.True(t, ok)

		// Re-check expiry
		// 6.5 tokens should take 3.25s to replenish at 2/sec
		require.WithinDuration(t, time.Now().Add(3250*time.Millisecond), fullAt, 50*time.Millisecond)
	})

	t.Run("FullAtNoOverflow", func(t *testing.T) {
		// Create a stupidly configured limiter
		lim, err := NewLocalLimiter(&LimiterOpts{
			RatePerSec: math.SmallestNonzeroFloat64,
			Capacity:   math.MaxInt,
		})
		require.NoError(t, err)

		now := time.Now()
		ok, wait, fullAt, err := lim.updateLocked(-math.MaxInt64)
		require.NoError(t, err)
		require.True(t, ok)
		require.Zero(t, wait)

		// Ensure fullAt has not wrapped around
		require.Greater(t, fullAt, now)

		// Ensure fullAt is really far in the future
		require.Greater(t, fullAt, time.Now().Add(100*365*24*time.Hour))
	})
}

func TestLocalKeyedLimiter(t *testing.T) {
	testKeyedLimiter_Common(t, func(opts LimiterOpts) (CacheableKeyedLimiter, error) {
		return NewLocalKeyedLimiter(opts)
	})

	// Ensures cache keys have an accurate TTL set, so they don't linger in the cache longer than necessary.
	// This is very similar, but higher level than the FullAt test on LocalLimiter
	t.Run("TTL", func(t *testing.T) {
		// capacity = 50, refills at 2/sec
		// -> 25 seconds to completely fill bucket from empty
		lim, err := NewLocalKeyedLimiter(LimiterOpts{
			RatePerSec: ruerate.Every(500 * time.Millisecond),
			Capacity:   50,
		})
		require.NoError(t, err)

		// Take 7 tokens, leaving 43 tokens in the bucket
		ok, wait, err := lim.AllowN(t.Context(), "key", 7)
		require.NoError(t, err)
		require.Zero(t, wait)
		require.True(t, ok)

		// Check TTL.
		// Refilling at 2/sec, the 7 tokens we took should take 3.5s to replenish
		// Therefore TTL should be 3.5s, as expired keys = full bucket

		// Allow a little slop (50ms) for test execution time
		_, expiresAt, ok := lim.limiters.Shard("key").Get("key")
		require.WithinDuration(t, time.Now().Add(3500*time.Millisecond), expiresAt, 50*time.Millisecond)

		// Now test to make sure TTLs accurate reflect token accrual of an *existing* bucket
		// There have been bugs here in the past.

		// Wait enough time for more tokens to replenish
		// 750ms = 1.5 tokens
		<-time.After(750 * time.Millisecond)

		// Take 1 token, theoretically leaving 43 + (1.5) - 1 = 43.5 tokens in the bucket
		ok, wait, err = lim.AllowN(t.Context(), "key", 1)
		require.NoError(t, err)
		require.Zero(t, wait)
		require.True(t, ok)

		// Re-check expiry
		// 6.5 tokens should take 3.25s to replenish at 2/sec
		_, expiresAt, ok = lim.limiters.Shard("key").Get("key")
		require.WithinDuration(t, time.Now().Add(3250*time.Millisecond), expiresAt, 50*time.Millisecond)
	})

}
