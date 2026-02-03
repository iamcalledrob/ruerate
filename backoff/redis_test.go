package backoff

import (
	"testing"
	"time"

	ruerate "github.com/iamcalledrob/ruebucket"
	"github.com/stretchr/testify/require"
)

func TestRedisLimiter(t *testing.T) {
	testLimiter_Common(t, func(opts LimiterOpts) (Limiter, error) {
		return NewRedisBackoffLimiter(ruerate.NewTestRedisClient(t), RedisLimiterOpts{
			LimiterOpts: opts,
			RedisKey:    "limiter:test",
		})
	})

	// Ensure keys exist in Redis and get set with expected TTLs
	t.Run("RedisKeys", func(t *testing.T) {
		const fixedKey = "KEY"

		opts := RedisKeyedLimiterOpts{
			LimiterOpts: LimiterOpts{
				PenaltyDecayInterval: 1000 * time.Millisecond,
			},
			RedisKey: func(key string) string {
				require.Equal(t, key, fixedKey)
				return "limiter:test:{" + key + "}"
			},
		}

		client := ruerate.NewTestRedisClient(t)
		l, err := NewRedisKeyedBackoffLimiter(client, opts)
		require.NoError(t, err)

		var redisTimeParts []int64
		redisTimeParts, err = client.Do(t.Context(), client.B().Time().Build()).AsIntSlice()
		require.NoError(t, err)
		redisNow := time.Unix(redisTimeParts[0], redisTimeParts[1]*int64(time.Microsecond))

		// Hit the limiter, increment penalty from 0 -> 1
		// Keys should be set with 100ms ttl (due to penalty now = 1)
		var ok bool
		ok, _, err = l.Allow(t.Context(), fixedKey)
		require.NoError(t, err)
		require.True(t, ok)

		redisKey := opts.RedisKey(fixedKey)

		// Retrieve both fields from the single Hash key
		// p = penalty, la = last_acquired_at
		res, err := client.Do(t.Context(), client.B().Hmget().Key(redisKey).Field("p", "la").Build()).ToArray()
		require.NoError(t, err)

		// Parse penalty from the first element
		gotPenaltyValue, err := res[0].AsFloat64()
		require.NoError(t, err)

		// Parse last_acquired_at from the second element
		gotLastAcquiredAtValue, err := res[1].AsInt64()
		require.NoError(t, err)

		// Get TTL for the single shared key
		gotTtlMs, err := client.Do(t.Context(), client.B().Pttl().Key(redisKey).Build()).AsInt64()
		require.NoError(t, err)

		// Penalty should be 1 -- a single hit to the limiter was made
		require.EqualValues(t, 1, gotPenaltyValue)

		// LastAcquiredAt should basically be the redisNow value, but allowing 10ms slop
		// for Redis RTT time. Assuming tests are run against a local Redis server.
		require.InDelta(t, redisNow.UnixMicro(), gotLastAcquiredAtValue, float64(10*time.Millisecond.Microseconds()))

		// TTLs should be 1000ms -- i.e. PenaltyDecayRate
		require.InDelta(t, 1000*time.Millisecond, time.Duration(gotTtlMs)*time.Millisecond, float64(10*time.Millisecond))

	})
}
