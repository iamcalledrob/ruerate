package tokenbucket

import (
	"context"
	"testing"
	"time"

	"github.com/iamcalledrob/ruebucket"
	"github.com/stretchr/testify/require"
)

func TestRedisLimiter(t *testing.T) {
	testLimiter_Common(t, func(opts LimiterOpts) (Limiter, error) {
		return NewRedisLimiter(ruerate.NewTestRedisClient(t), RedisLimiterOpts{
			LimiterOpts: opts,
			RedisKey:    "limiter:test",
		})
	})

	// Ensures keys have an accurate TTL set, so they don't linger in redis longer than necessary.
	// Not strictly necessary, but good to catch bugs
	t.Run("TTL", func(t *testing.T) {
		// capacity = 50, refills at 25/sec
		// -> 2 seconds to completely fill bucket from empty
		client := ruerate.NewTestRedisClient(t)
		opts := RedisLimiterOpts{
			LimiterOpts: LimiterOpts{
				RatePerSec: ruerate.Every(40 * time.Millisecond),
				Capacity:   50,
			},
			RedisKey: "limiter:test",
		}
		lim, err := NewRedisLimiter(client, opts)
		require.NoError(t, err)

		// Take 7 tokens, leaving 43 tokens in the bucket
		ok, wait, err := lim.AllowN(t.Context(), 7)
		require.NoError(t, err)
		require.Zero(t, wait)
		require.True(t, ok)

		// Check TTL.
		// Refilling at 25/sec, the 7 tokens we took should take 0.28s (7/25) to replenish
		// Therefore TTL should be 0.28, as expired keys = full bucket
		var msec int64
		resp := client.Do(t.Context(), client.B().Pttl().Key(opts.RedisKey).Build())
		msec, err = resp.AsInt64()
		require.NoError(t, err)

		// Allow 20ms slop for localhost roundtrip
		if msec < 260 || msec > 300 {
			t.Fatalf("TTL out of bounds: ms = %d", msec)
		}
	})

	t.Run("Caching", func(t *testing.T) {
		client := ruerate.NewTestRedisClient(t)
		lim, err := NewRedisCacheableKeyedLimiter(client, RedisKeyedLimiterOpts{
			LimiterOpts: LimiterOpts{
				RatePerSec: ruerate.Every(100 * time.Millisecond),
				Capacity:   1,
			},
			RedisKey: func(key string) string {
				return "limiter:" + key
			},
		})
		require.NoError(t, err)

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
	})
}

func BenchmarkRedisLimiter(b *testing.B) {
	type Allower interface {
		Allow(context.Context) (bool, time.Duration, error)
	}

	test := func(b *testing.B, p int, lim Allower) {
		b.ReportAllocs()
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

	// Cacheable limiter locally caches the exhausted state of the limiter and wait time
	b.Run("Cacheable", func(b *testing.B) {
		b.Run("Exhausted_Serial", func(b *testing.B) {
			lim, err := NewRedisCacheableLimiterWithDefaultKey(ruerate.NewTestRedisClient(b), "id", LimiterOpts{
				RatePerSec: ruerate.Every(100 * time.Millisecond),
				Capacity:   1,
			})
			require.NoError(b, err)
			test(b, 1, lim)
		})
		b.Run("Exhausted_Parallel100", func(b *testing.B) {
			lim, err := NewRedisCacheableLimiterWithDefaultKey(ruerate.NewTestRedisClient(b), "id", LimiterOpts{
				RatePerSec: ruerate.Every(1 * time.Hour),
				Capacity:   1,
			})
			require.NoError(b, err)
			test(b, 100, lim)
		})

		// Tests the "allowed" path
		b.Run("Allowed_Serial", func(b *testing.B) {
			lim, err := NewRedisCacheableLimiterWithDefaultKey(ruerate.NewTestRedisClient(b), "id", LimiterOpts{
				RatePerSec: ruerate.Every(1 * time.Hour),
				Capacity:   b.N,
			})
			require.NoError(b, err)
			test(b, 1, lim)
		})
		b.Run("Allowed_Parallel100", func(b *testing.B) {
			lim, err := NewRedisCacheableLimiterWithDefaultKey(ruerate.NewTestRedisClient(b), "id", LimiterOpts{
				RatePerSec: ruerate.Every(1 * time.Hour),
				Capacity:   b.N,
			})
			require.NoError(b, err)
			test(b, 100, lim)
		})
	})

	// Tests the "exhausted cache" path -- different perf profile in token bucket lua
	b.Run("NonCachable", func(b *testing.B) {
		b.Run("Exhausted_Serial", func(b *testing.B) {
			lim, err := NewRedisLimiterWithDefaultKey(ruerate.NewTestRedisClient(b), "id", LimiterOpts{
				RatePerSec: ruerate.Every(100 * time.Millisecond),
				Capacity:   1,
			})
			require.NoError(b, err)
			test(b, 1, lim)
		})
		b.Run("Exhausted_Parallel100", func(b *testing.B) {
			lim, err := NewRedisLimiterWithDefaultKey(ruerate.NewTestRedisClient(b), "id", LimiterOpts{
				RatePerSec: ruerate.Every(1 * time.Hour),
				Capacity:   1,
			})
			require.NoError(b, err)
			test(b, 100, lim)
		})

		// Tests the "allowed" path
		b.Run("Allowed_Serial", func(b *testing.B) {
			lim, err := NewRedisLimiterWithDefaultKey(ruerate.NewTestRedisClient(b), "id", LimiterOpts{
				RatePerSec: ruerate.Every(1 * time.Hour),
				Capacity:   b.N,
			})
			require.NoError(b, err)
			test(b, 1, lim)
		})
		b.Run("Allowed_Parallel100", func(b *testing.B) {
			lim, err := NewRedisLimiterWithDefaultKey(ruerate.NewTestRedisClient(b), "id", LimiterOpts{
				RatePerSec: ruerate.Every(1 * time.Hour),
				Capacity:   b.N,
			})
			require.NoError(b, err)
			test(b, 100, lim)
		})
	})

	// The bulk of the overhead is invoking a lua script, which seems to be ~18,000ns/op
	// Still, that's about 55K ops/sec which is screaming.
}
