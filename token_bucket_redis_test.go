package ruebucket

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

// Tests run against a locally running redis/valkey instance, selecting database 1.
// Note: Completely erases the database before each test!

func NewTestClient(t testing.TB) rueidis.Client {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{addr},
		SelectDB:    1, // don't clobber default db (0)
	})
	require.NoError(t, err)

	// Erase the entire db to isolate tests from each other.
	err = client.Do(t.Context(), client.B().Flushdb().Build()).Error()
	require.NoError(t, err)

	return client
}

func TestLimiter(t *testing.T) {
	client := NewTestClient(t)
	lim := NewLimiter(client, "id", AllowEvery(50*time.Millisecond), 3)

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
	// Ensure the wait time is as expected: slightly less than 50ms
	ok, wait, err = lim.Allow(t.Context())
	require.NoError(t, err)
	require.Less(t, wait, 50*time.Millisecond)
	require.Greater(t, wait, 40*time.Millisecond)
	require.False(t, ok)

	// Wait until another token will have been added to the bucket
	// Ensure it's possible to allow again
	<-time.After(50 * time.Millisecond)
	ok, wait, err = lim.Allow(t.Context())
	require.NoError(t, err)
	require.Zero(t, wait)
	require.True(t, ok)

	// Wait for bucket to be completely full
	// Ensure that no key remains in redis -- it's cleaned up automatically via PX
	<-time.After(3 * 50 * time.Millisecond)

	// Ensure tokensKey and lastAcquiredAtKey don't exist
	ok, err = keyExists(t.Context(), client, lim.source.replenishable.tokensKey("default"))
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = keyExists(t.Context(), client, lim.source.replenishable.lastAcquiredAtKey("default"))
	require.NoError(t, err)
	require.False(t, ok)
}

// Ensures that the limiter's underlying bucket is constrained between [0<Capacity] tokens
func TestLimiter_Bounds(t *testing.T) {
	client := NewTestClient(t)
	lim := NewReplenishableKeyedLimiter(client, "id", AllowEvery(40*time.Millisecond), 50)

	// Request more tokens than could ever be granted
	ok, wait, err := lim.AllowN(t.Context(), "key", 100)
	require.ErrorIs(t, err, ErrExceedsBucketCapacity)
	require.Zero(t, wait)
	require.False(t, ok)
}

// Ensures keys have an accurate TTL set, so they don't linger in redis longer than necessary.
// Not strictly necessary, but good to catch bugs
func TestLimiter_TTL(t *testing.T) {
	// capacity = 50, refills at 25/sec
	// -> 2 seconds to completely fill bucket from empty
	client := NewTestClient(t)
	lim := NewReplenishableKeyedLimiter(client, "id", AllowEvery(40*time.Millisecond), 50)

	// Take 7 tokens, leaving 43 tokens in the bucket
	ok, wait, err := lim.AllowN(t.Context(), "key", 7)
	require.NoError(t, err)
	require.Zero(t, wait)
	require.True(t, ok)

	// Check TTL.
	// Refilling at 25/sec, the 7 tokens we took should take 0.28s (7/25) to replenish
	// Therefore TTL should be 0.28, as expired keys = full bucket
	var msec int64
	resp := client.Do(t.Context(), client.B().Pttl().Key(lim.tokensKey("key")).Build())
	msec, err = resp.AsInt64()
	require.NoError(t, err)

	// Allow 20ms slop for localhost roundtrip
	if msec < 260 || msec > 300 {
		t.Fatalf("TTL out of bounds: ms = %d", msec)
	}
}

func TestLimiter_ReplenishTooMany(t *testing.T) {
	client := NewTestClient(t)
	lim := NewReplenishableKeyedLimiter(client, "id", AllowEvery(1*time.Second), 1)

	// Ensure replenishing too many tokens does not overflow the bucket
	// Replenishing additional tokens should noop
	err := lim.ReplenishN(t.Context(), "key", 10)
	require.NoError(t, err)

	// Ensure 1 token (bucket capacity) can still be got
	var ok bool
	var wait time.Duration
	ok, wait, err = lim.Allow(t.Context(), "key")
	require.NoError(t, err)
	require.Zero(t, wait)
	require.True(t, ok)

	// Ensure further tokens can't (i.e. bucket didn't go negative)
	ok, _, err = lim.Allow(t.Context(), "key")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestLimiter_Replenish(t *testing.T) {
	client := NewTestClient(t)
	lim := NewReplenishableKeyedLimiter(client, "id", AllowEvery(1*time.Second), 1)

	// Exhaust the limiter
	ok, wait, err := lim.Allow(t.Context(), "key")
	require.NoError(t, err)
	require.Zero(t, wait)
	require.True(t, ok)

	// Ensure limiter is exhausted
	ok, wait, err = lim.Allow(t.Context(), "key")
	require.NoError(t, err)
	require.NotZero(t, wait)
	require.False(t, ok)

	// Ensure the replenish succeeds (should always succeed)
	err = lim.Replenish(t.Context(), "key")
	require.NoError(t, err)

	// Ensure limiter is no longer exhausted
	ok, wait, err = lim.Allow(t.Context(), "key")
	require.NoError(t, err)
	require.Zero(t, wait)
	require.True(t, ok)
}

// Ensures that limiters with distinct ids act independently
func TestLimiter_DistinctIds(t *testing.T) {
	client := NewTestClient(t)

	lim1 := NewLimiter(client, "id1", AllowEvery(40*time.Millisecond), 1)
	lim2 := NewLimiter(client, "id2", AllowEvery(40*time.Millisecond), 1)

	// Exhaust lim1
	ok, _, err := lim1.Allow(t.Context())
	require.NoError(t, err)
	require.True(t, ok)
	// Ensure lim1 is exhausted
	ok, _, err = lim1.Allow(t.Context())
	require.NoError(t, err)
	require.False(t, ok)

	// Ensure lim2 is not exhausted
	ok, _, err = lim2.Allow(t.Context())
	require.NoError(t, err)
	require.True(t, ok)
}

func TestLimiter_DistinctKeys(t *testing.T) {
	client := NewTestClient(t)
	lim := NewKeyedLimiter(client, "id", AllowEvery(40*time.Millisecond), 1)

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

func TestLimiter_WaitCaching(t *testing.T) {
	client := NewTestClient(t)
	lim := NewKeyedLimiter(client, "id", AllowEvery(100*time.Millisecond), 1)

	// Exhaust key1
	ok, wait, err := lim.Allow(t.Context(), "key1")
	require.NoError(t, err)
	require.Zero(t, wait)
	require.True(t, ok)

	require.EqualValues(t, 0, lim.cacheHits.Load())
	require.EqualValues(t, 1, lim.cacheMisses.Load())

	// Ensure key1 is exhausted (wait should be now cached)
	ok, wait, err = lim.Allow(t.Context(), "key1")
	require.NoError(t, err)
	require.NotZero(t, wait)
	require.False(t, ok)

	require.EqualValues(t, 0, lim.cacheHits.Load())
	require.EqualValues(t, 2, lim.cacheMisses.Load())

	// The deadline expected for any subsequent call to Allow for the same key
	cachedWaitExpiry := time.Now().Add(wait)

	// Try and allow for key1 again.
	// Ensure that the cached wait is used
	ok, wait, err = lim.Allow(t.Context(), "key1")
	require.NoError(t, err)
	require.False(t, ok)

	// Ensure the wait time returned is consistent with the prior Allow (allow some slop)
	require.WithinDuration(t, cachedWaitExpiry, time.Now().Add(wait), 10*time.Millisecond)

	// Ensure cache was used
	require.EqualValues(t, 1, lim.cacheHits.Load())
	require.EqualValues(t, 2, lim.cacheMisses.Load())

	// Ensure that after "wait", it's possible to allow for the same key
	<-time.After(wait)
	ok, wait, err = lim.Allow(t.Context(), "key1")
	require.NoError(t, err)
	require.Zero(t, wait)
	require.True(t, ok)

	// Ensure the cached wait value is no longer being used
	// Should inherently be true if the Allow above succeeded
	require.EqualValues(t, 1, lim.cacheHits.Load())
	require.EqualValues(t, 3, lim.cacheMisses.Load())

	// Ensure the entry in the wait cache has been cleaned up
	require.False(t, lim.waitCache.Has("key1"))
}

func BenchmarkLimiter(b *testing.B) {
	type Allower interface {
		Allow(context.Context) (bool, time.Duration, error)
	}

	test := func(b *testing.B, p int, lim Allower) {
		b.SetParallelism(p)
		b.ResetTimer()

		// In parallel because rueidis
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _, _ = lim.Allow(b.Context())
			}
		})
	}

	// Parallel branches used to demonstrate that rueidis auto-pipelining is working.

	// Limiter locally caches the exhausted state of the limiter and wait time
	b.Run("Limiter", func(b *testing.B) {
		b.Run("Exhausted_Serial", func(b *testing.B) {
			lim := NewLimiter(NewTestClient(b), "id", AllowEvery(time.Hour), 1)
			test(b, 1, lim)
		})
		b.Run("Exhausted_Parallel100", func(b *testing.B) {
			lim := NewLimiter(NewTestClient(b), "id", AllowEvery(time.Hour), 1)
			test(b, 100, lim)
		})

		// Tests the "allowed" path
		b.Run("Allowed_Serial", func(b *testing.B) {
			lim := NewReplenishableLimiter(NewTestClient(b), "id", AllowEvery(time.Hour), b.N)
			test(b, 1, lim)
		})
		b.Run("Allowed_Parallel100", func(b *testing.B) {
			lim := NewReplenishableLimiter(NewTestClient(b), "id", AllowEvery(time.Hour), b.N)
			test(b, 100, lim)
		})
	})

	// ReplenishableLimiter doesn't cache
	// Tests the "exhausted cache" path -- different perf profile in token bucket lua
	b.Run("Replenishable", func(b *testing.B) {
		b.Run("Exhausted_Serial", func(b *testing.B) {
			lim := NewReplenishableLimiter(NewTestClient(b), "id", AllowEvery(time.Hour), 1)
			test(b, 1, lim)
		})
		b.Run("Exhausted_Parallel100", func(b *testing.B) {
			lim := NewReplenishableLimiter(NewTestClient(b), "id", AllowEvery(time.Hour), 1)
			test(b, 100, lim)
		})

		// Tests the "allowed" path
		b.Run("Allowed_Serial", func(b *testing.B) {
			lim := NewReplenishableLimiter(NewTestClient(b), "id", AllowEvery(time.Hour), b.N)
			test(b, 1, lim)
		})
		b.Run("Allowed_Parallel100", func(b *testing.B) {
			lim := NewReplenishableLimiter(NewTestClient(b), "id", AllowEvery(time.Hour), b.N)
			test(b, 100, lim)
		})
	})

	// The bulk of the overhead is invoking a lua script, which seems to be ~18,000ns/op
	// Still, that's about 55K ops/sec which is screaming.
}

func keyExists(ctx context.Context, client rueidis.Client, key string) (bool, error) {
	return client.Do(ctx, client.B().Exists().Key(key).Build()).AsBool()
}
