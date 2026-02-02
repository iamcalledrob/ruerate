package ruebucket

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Tests mostly copy+paste from limiter_test.go
// Adapted for different signature (no ctx, fewer possible errors)

func TestLocalLimiter(t *testing.T) {
	lim := NewLocalLimiter(AllowEvery(50*time.Millisecond), 3)

	// Hit the limiter 3 times (burst=3) to exhaust it
	// Ensure it's possible to allow all 3 times
	ok, wait := lim.Allow()
	require.Zero(t, wait)
	require.True(t, ok)

	ok, wait = lim.Allow()
	require.Zero(t, wait)
	require.True(t, ok)

	ok, wait = lim.Allow()
	require.Zero(t, wait)
	require.True(t, ok)

	// Ensure it's *not* possible to allow a 4th time, as limit is met
	// Ensure the wait time is as expected: less than 50ms.
	// (test for Less or Equal due to imprecision)
	ok, wait = lim.Allow()
	require.LessOrEqual(t, wait, 50*time.Millisecond)
	require.Greater(t, wait, 40*time.Millisecond)
	require.False(t, ok)

	// Wait until another token will have been added to the bucket
	// Ensure it's possible to allow again
	<-time.After(50 * time.Millisecond)
	ok, wait = lim.Allow()
	require.Zero(t, wait)
	require.True(t, ok)
}

// Ensures that the limiter's underlying bucket is constrained between [0<Capacity] tokens
func TestLocalLimiter_Bounds(t *testing.T) {
	lim := NewLocalLimiter(AllowEvery(40*time.Millisecond), 50)

	// Request more tokens than could ever be granted
	ok, wait, err := lim.AllowN(100)
	require.ErrorIs(t, err, ErrExceedsBucketCapacity)
	require.Zero(t, wait)
	require.False(t, ok)
}

// Ensures keys have an accurate TTL set, so they don't linger in ttlcache longer than necessary.
func TestLocalLimiter_TTL(t *testing.T) {
	// capacity = 50, refills at 25/sec
	// -> 2 seconds to completely fill bucket from empty
	lim := NewLocalKeyedLimiter(AllowEvery(40*time.Millisecond), 50)

	// Take 7 tokens, leaving 43 tokens in the bucket
	ok, wait, err := lim.AllowN("key", 7)
	require.NoError(t, err)
	require.Zero(t, wait)
	require.True(t, ok)

	// Check TTL.
	// Refilling at 25/sec, the 7 tokens we took should take 0.28s (7/25) to replenish
	// Therefore TTL should be 0.28, as expired keys = full bucket

	// Allow 2ms slop for slow execution
	entry := lim.buckets.Get("key")
	msec := entry.TTL().Milliseconds()
	if msec < 278 || msec > 282 {
		t.Fatalf("TTL out of bounds: ms = %d", msec)
	}
}

func TestLocalLimiter_ReplenishTooMany(t *testing.T) {
	lim := NewLocalKeyedLimiter(AllowEvery(1*time.Second), 1)

	// Ensure replenishing too many tokens does not overflow the bucket
	// Replenishing additional tokens should noop
	lim.ReplenishN("key", 10)

	// Ensure 1 token (bucket capacity) can still be got
	ok, wait, err := lim.Allow("key")
	require.NoError(t, err)
	require.Zero(t, wait)
	require.True(t, ok)

	// Ensure further tokens can't (i.e. bucket didn't go negative)
	ok, _, err = lim.Allow("key")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestLocalLimiter_Replenish(t *testing.T) {
	lim := NewLocalKeyedLimiter(AllowEvery(1*time.Second), 1)

	// Exhaust the limiter
	ok, wait, err := lim.Allow("key")
	require.NoError(t, err)
	require.Zero(t, wait)
	require.True(t, ok)

	// Ensure limiter is exhausted
	ok, wait, err = lim.Allow("key")
	require.NoError(t, err)
	require.NotZero(t, wait)
	require.False(t, ok)

	lim.Replenish("key")

	// Ensure limiter is no longer exhausted
	ok, wait, err = lim.Allow("key")
	require.NoError(t, err)
	require.Zero(t, wait)
	require.True(t, ok)
}

func TestLocalLimiter_DistinctKeys(t *testing.T) {
	lim := NewLocalKeyedLimiter(AllowEvery(40*time.Millisecond), 1)

	// Exhaust key1
	ok, _, err := lim.Allow("key1")
	require.NoError(t, err)
	require.True(t, ok)
	// Ensure key1 is exhausted
	ok, _, err = lim.Allow("key1")
	require.NoError(t, err)
	require.False(t, ok)

	// Ensure key2 is not exhausted
	ok, _, err = lim.Allow("key2")
	require.NoError(t, err)
	require.True(t, ok)
}
