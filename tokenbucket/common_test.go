package tokenbucket

import (
	"math"
	"testing"
	"time"

	ruerate "github.com/iamcalledrob/ruebucket"
	"github.com/stretchr/testify/require"
)

// Currently, tokenbucket tests only use the real clock, and don't inject time
// Future improvement: use injected time, as with backoff limiter.

func testLimiter_Common(
	t *testing.T,
	factory func(opts LimiterOpts) (Limiter, error),
) {
	t.Run("BasicFunctionality", func(t *testing.T) {
		testLimiter_BasicFunctionality(t, func(opts LimiterOpts) (CacheableLimiter, error) {
			return factory(opts)
		})
	})
	t.Run("Bounds", func(t *testing.T) {
		testLimiter_Bounds(t, factory)
	})
	t.Run("ReplenishTooMany", func(t *testing.T) {
		testLimiter_ReplenishTooMany(t, factory)
	})
	t.Run("Replenish", func(t *testing.T) {
		testLimiter_Replenish(t, factory)
	})
	t.Run("Overflows", func(t *testing.T) {
		testLimiter_Overflows(t, factory)
	})
}

func testKeyedLimiter_Common(
	t *testing.T,
	factory func(opts LimiterOpts) (CacheableKeyedLimiter, error),
) {
	t.Run("DistinctKeys", func(t *testing.T) {
		testKeyedLimiter_DistinctKeys(t, factory)
	})
}

func testLimiter_BasicFunctionality(
	t *testing.T,
	factory func(opts LimiterOpts) (CacheableLimiter, error),
) {
	lim, err := factory(LimiterOpts{
		RatePerSec: ruerate.Every(50 * time.Millisecond),
		Capacity:   3,
	})
	require.NoError(t, err)

	// Hit the limiter 3 times (burst=3) to exhaust it
	// Ensure it's possible to allow all 3 times
	ok, wait, err := lim.Allow(t.Context())
	require.NoError(t, err)
	require.Zero(t, wait)
	require.True(t, ok)

	ok, wait, err = lim.Allow(t.Context())
	require.NoError(t, err)
	require.Zero(t, wait)
	require.True(t, ok)

	ok, wait, err = lim.Allow(t.Context())
	require.NoError(t, err)
	require.Zero(t, wait)
	require.True(t, ok)

	// Ensure it's *not* possible to allow a 4th time, as limit is met
	// Ensure the wait time is as expected: less than 50ms.
	// (test for Less or Equal due to imprecision)
	ok, wait, err = lim.Allow(t.Context())
	require.NoError(t, err)
	require.LessOrEqual(t, wait, 50*time.Millisecond)
	require.Greater(t, wait, 40*time.Millisecond)
	require.False(t, ok)

	// Wait until another token will have been added to the bucket
	// Ensure it's possible to allow again
	<-time.After(50 * time.Millisecond)
	ok, wait, err = lim.Allow(t.Context())
	require.NoError(t, err)
	require.Zero(t, wait)
	require.True(t, ok)
}

// Ensures that the limiter's underlying bucket is constrained between [0<Capacity] tokens
func testLimiter_Bounds(
	t *testing.T,
	factory func(opts LimiterOpts) (Limiter, error),
) {
	lim, err := factory(LimiterOpts{
		RatePerSec: ruerate.Every(40 * time.Millisecond),
		Capacity:   50,
	})
	require.NoError(t, err)

	// Request more tokens than could ever be granted
	ok, wait, err := lim.AllowN(t.Context(), 100)
	require.ErrorIs(t, err, ErrExceedsBucketCapacity)
	require.Zero(t, wait)
	require.False(t, ok)
}

func testLimiter_ReplenishTooMany(
	t *testing.T,
	factory func(opts LimiterOpts) (Limiter, error),
) {
	lim, err := factory(LimiterOpts{
		RatePerSec: ruerate.Every(1 * time.Second),
		Capacity:   1,
	})
	require.NoError(t, err)

	// Ensure replenishing too many tokens does not overflow the bucket
	// Replenishing additional tokens should noop
	err = lim.ReplenishN(t.Context(), 10)
	require.NoError(t, err)

	// Ensure 1 token (bucket capacity) can still be got
	ok, wait, err := lim.Allow(t.Context())
	require.NoError(t, err)
	require.Zero(t, wait)
	require.True(t, ok)

	// Ensure further tokens can't (i.e. bucket didn't go negative)
	ok, _, err = lim.Allow(t.Context())
	require.NoError(t, err)
	require.False(t, ok)
}

func testLimiter_Replenish(
	t *testing.T,
	factory func(opts LimiterOpts) (Limiter, error),
) {
	lim, err := factory(LimiterOpts{
		RatePerSec: ruerate.Every(1 * time.Second),
		Capacity:   1,
	})
	require.NoError(t, err)

	// Exhaust the limiter
	ok, wait, err := lim.Allow(t.Context())
	require.NoError(t, err)
	require.Zero(t, wait)
	require.True(t, ok)

	// Ensure limiter is exhausted
	ok, wait, err = lim.Allow(t.Context())
	require.NoError(t, err)
	require.NotZero(t, wait)
	require.False(t, ok)

	err = lim.Replenish(t.Context())
	require.NoError(t, err)

	// Ensure limiter is no longer exhausted
	ok, wait, err = lim.Allow(t.Context())
	require.NoError(t, err)
	require.Zero(t, wait)
	require.True(t, ok)
}

func testLimiter_Overflows(
	t *testing.T,
	factory func(opts LimiterOpts) (Limiter, error),
) {
	lim, err := factory(LimiterOpts{
		RatePerSec: math.SmallestNonzeroFloat64,
		Capacity:   math.MaxInt,
	})
	require.NoError(t, err)

	// Almost exhaust the limiter
	ok, wait, err := lim.AllowN(t.Context(), math.MaxInt-100)
	require.NoError(t, err)
	require.True(t, ok)

	// Actually exhaust the limiter
	// 2 steps so both the zero and non-zero states are considered
	// There will be some internal precision loss at these scales
	ok, wait, err = lim.AllowN(t.Context(), 1000)
	require.NoError(t, err)
	require.False(t, ok)

	// Ensure wait has not wrapped around to be negative
	require.Greater(t, wait, time.Duration(0))

	// Sanity check: ensure the wait is really, really big.
	// Checks for 100 years, but will be more like heat death of the universe.
	require.Greater(t, wait, 100*365*24*time.Hour)
}

func testKeyedLimiter_DistinctKeys(
	t *testing.T,
	factory func(opts LimiterOpts) (CacheableKeyedLimiter, error),
) {
	lim, err := factory(LimiterOpts{
		RatePerSec: ruerate.Every(40 * time.Millisecond),
		Capacity:   1,
	})
	require.NoError(t, err)

	// Exhaust key1
	ok, _, err := lim.Allow(t.Context(), "key1")
	require.NoError(t, err)
	require.True(t, ok)
	// Ensure key1 is exhausted
	ok, _, err = lim.Allow(t.Context(), "key1")
	require.NoError(t, err)
	require.False(t, ok)

	// Ensure key2 is not exhausted
	ok, _, err = lim.Allow(t.Context(), "key2")
	require.NoError(t, err)
	require.True(t, ok)
}
