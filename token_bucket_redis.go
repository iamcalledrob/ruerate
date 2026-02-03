package ruerate

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"github.com/redis/rueidis"
)

// ReplenishableKeyedLimiter is a redis-backed token bucket rate limiter that supports attempting to take tokens
// from the bucket AND replenishing tokens back into the bucket.
//
// The limiter is identified by "id", and Allow/Replenish take a key used to identify an individual resource.
// This allows a single limiter instance to limit multiple an action for multiple different actors,
// e.g. limiting a common action (id: api_endpoint) on a per-actor basis (key: ip_address).
//
// Using the same id and limit+burst configuration allows multiple machines to participate in the same limiter.
//
// Each call to AllowN and ReplenishN is a round-trip to redis -- there is no caching. All keys written to Redis
// expire via PX, and the "full bucket" state is reflected with no keys existing in redis.
//
// Use KeyedLimiter instead for scenarios where replenishment and taking multiple tokens aren't needed,
// as KeyedLimiter can cache client-side.
type ReplenishableKeyedLimiter struct {
	client rueidis.Client

	id             string
	ratePerSec     float64
	bucketCapacity int
}

func NewReplenishableKeyedLimiter(client rueidis.Client, id string, ratePerSec float64, bucketCapacity int) *ReplenishableKeyedLimiter {
	return &ReplenishableKeyedLimiter{client: client, id: id, ratePerSec: ratePerSec, bucketCapacity: bucketCapacity}
}

func (l *ReplenishableKeyedLimiter) Replenish(ctx context.Context, key string) error {
	return l.ReplenishN(ctx, key, 1)
}

func (l *ReplenishableKeyedLimiter) Allow(ctx context.Context, key string) (ok bool, wait time.Duration, err error) {
	return l.AllowN(ctx, key, 1)
}

func (l *ReplenishableKeyedLimiter) ReplenishN(ctx context.Context, key string, n int) error {
	_, _, err := l.AllowN(ctx, key, -n)
	return err
}

func (l *ReplenishableKeyedLimiter) AllowN(ctx context.Context, key string, n int) (ok bool, wait time.Duration, err error) {
	if n > l.bucketCapacity {
		err = fmt.Errorf("%w: n (%d) > capacity (%d)", ErrExceedsBucketCapacity, n, l.bucketCapacity)
		return
	}

	limitString := strconv.FormatFloat(l.ratePerSec, 'f', -1, 64)

	resp := luaTokenBucket.Exec(
		ctx,
		l.client,
		[]string{
			l.tokensKey(key),         // lua: tokens_key
			l.lastAcquiredAtKey(key), // lua: last_acquired_at_key
		},
		[]string{
			limitString,                    // lua: rate_per_sec
			strconv.Itoa(l.bucketCapacity), // lua: bucket_capacity
			strconv.Itoa(-n),               // lua: tokens_delta (acquiring 1 = delta of -1)
		},
	)

	// Redis lua script should respond with array of 2 ints
	var ints []int64
	ints, err = resp.AsIntSlice()
	if err != nil {
		err = fmt.Errorf("limiter %s: acquiring key %s: %w", l.id, key, err)
		return
	}
	if len(ints) != 2 {
		err = fmt.Errorf("limiter %s: incorrect result length: %d", l.id, len(ints))
		return
	}

	allowed := ints[0] == 1
	if allowed {
		ok = true
		return
	}

	// Convert micros to nanos (time.Duration)
	wait = time.Duration(ints[1] * 1000)

	// Sanity check
	if wait < 1 {
		err = fmt.Errorf("bug: failed to acquire but no wait")
		return
	}

	return
}

func (l *ReplenishableKeyedLimiter) tokensKey(key string) string {
	return "limiter:" + l.id + ":{" + key + "}:tokens"
}

func (l *ReplenishableKeyedLimiter) lastAcquiredAtKey(key string) string {
	return "limiter:" + l.id + ":{" + key + "}:last_acquired"
}

// KeyedLimiter is a non-replenishable limiter that caches wait durations to avoid unnecessary redis calls.
//
// If an earlier call to Allow was unsuccessful due to an empty bucket, it returns the wait time until the next
// token will be available. The limiter won't make further round-trips to redis until the wait time has elapsed.
type KeyedLimiter struct {
	replenishable *ReplenishableKeyedLimiter
	waitCache     *ttlcache.Cache[string, struct{}]

	// Currently just for tests
	cacheHits   atomic.Int64
	cacheMisses atomic.Int64
}

// NewKeyedLimiter instantiates the limiter and starts a goroutine to clean up expired wait cache items.
// Call Stop when done.
func NewKeyedLimiter(client rueidis.Client, id string, ratePerSec float64, bucketCapacity int) *KeyedLimiter {
	// waitCache uses ttlcache as a self-cleaning map of [string]time.Time, by stuffing an empty struct into
	// the map value and treating the TTL/key expiry as the real value.
	//
	// keys stay in the map up until their ttl expires.
	waitCache := ttlcache.New(ttlcache.WithDisableTouchOnHit[string, struct{}]())
	go waitCache.Start()

	return &KeyedLimiter{
		replenishable: NewReplenishableKeyedLimiter(client, id, ratePerSec, bucketCapacity),
		waitCache:     waitCache,
	}
}

// Stop stops automatically cleaning up the internal wait cache
// Call when no longer using KeyedLimiter to avoid a goroutine leak.
func (l *KeyedLimiter) Stop() {
	l.waitCache.Stop()
}

func (l *KeyedLimiter) Allow(ctx context.Context, key string) (ok bool, wait time.Duration, err error) {
	cacheEntry := l.waitCache.Get(key)

	if cacheEntry != nil && time.Now().Before(cacheEntry.ExpiresAt()) {
		l.cacheHits.Add(1)
		wait = time.Until(cacheEntry.ExpiresAt())
		return
	}

	l.cacheMisses.Add(1)

	ok, wait, err = l.replenishable.AllowN(ctx, key, 1)
	if err != nil {
		return
	}

	if !ok {
		l.waitCache.Set(key, struct{}{}, wait)
		return
	}

	// Acquired
	return
}

// ReplenishableLimiter is a convenience wrapper around a ReplenishableLimiter that limits a single default key
// Exists only for tests at the moment as it provides a common Allow() interface for benchmarks.
type ReplenishableLimiter struct {
	source *ReplenishableKeyedLimiter
}

func NewReplenishableLimiter(client rueidis.Client, id string, ratePerSec float64, bucketCapacity int) *ReplenishableLimiter {
	return &ReplenishableLimiter{source: NewReplenishableKeyedLimiter(client, id, ratePerSec, bucketCapacity)}
}

func (l *ReplenishableLimiter) Allow(ctx context.Context) (ok bool, wait time.Duration, err error) {
	return l.source.Allow(ctx, "__default__")
}

func (l *ReplenishableLimiter) Replenish(ctx context.Context) error {
	return l.source.Replenish(ctx, "__default__")
}

// Limiter is a convenience wrapper around a KeyedLimiter that limits a single default key
// Caches wait times when the limiter is exhausted.
type Limiter struct {
	source *KeyedLimiter
}

func NewLimiter(client rueidis.Client, id string, ratePerSec float64, bucketCapacity int) *Limiter {
	return &Limiter{source: NewKeyedLimiter(client, id, ratePerSec, bucketCapacity)}
}

func (l *Limiter) Allow(ctx context.Context) (ok bool, wait time.Duration, err error) {
	return l.source.Allow(ctx, "__default__")
}

//go:embed token_bucket.lua
var luaTokenBucketScript string
var luaTokenBucket = rueidis.NewLuaScript(luaTokenBucketScript)

// ErrExceedsBucketCapacity means the number of tokens requested from the limiter exceeds the capacity of the
// bucket and therefore can never be satisfied
var ErrExceedsBucketCapacity = errors.New("request exceeds bucket capacity")

// Every converts a time interval to a rate per second for use with *Limiter constructors.
// Similar to rate.Every, but avoids taking a dependency on that package.
func Every(interval time.Duration) float64 {
	if interval <= 0 {
		return math.MaxFloat64
	}
	return 1 / interval.Seconds()
}

var AllowEvery = Every // Backwards compat
