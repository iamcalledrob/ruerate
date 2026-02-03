package tokenbucket

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"github.com/redis/rueidis"
)

type RedisKeyedLimiterOpts struct {
	LimiterOpts LimiterOpts

	// RedisKey is the Redis key where the limiter state score should be stored
	// for a given limiter key, e.g. limiter:users_by_ip:{key}
	RedisKey func(key string) string
}

func (o *RedisKeyedLimiterOpts) Sanitize() error {
	if o.RedisKey == nil {
		return fmt.Errorf("RedisKey is required")
	}
	err := o.LimiterOpts.Sanitize()
	if err != nil {
		return fmt.Errorf("LimiterOpts: %w", err)
	}
	return nil
}

// RedisKeyedLimiter is a redis-backed token bucket rate limiter that supports attempting to take tokens
// from the bucket AND replenishing tokens back into the bucket.
//
// Allow/Replenish take a key used to identify an individual resource.
// This allows a single limiter instance to limit multiple an action for multiple different actors,
// e.g. limiting a common action (id: api_endpoint) on a per-actor basis (key: ip_address).
//
// Using the same opts allows multiple machines to participate in the same limiter.
//
// Each call to AllowN and ReplenishN is a round-trip to redis -- there is no caching. All keys written to Redis
// expire via PX, and the "full bucket" state is reflected with no keys existing in redis.
//
// Use RedisCacheableKeyedLimiter instead for scenarios where replenishment and taking multiple
// tokens aren't needed, as KeyedLimiter can cache client-side.
type RedisKeyedLimiter struct {
	client rueidis.Client
	opts   RedisKeyedLimiterOpts
	script *rueidis.Lua
}

func NewRedisKeyedLimiter(client rueidis.Client, opts RedisKeyedLimiterOpts) (*RedisKeyedLimiter, error) {
	err := opts.Sanitize()
	if err != nil {
		return nil, fmt.Errorf("opts: %w", err)
	}

	// Opts don't change during limiter lifespan, more efficient to define as constants.
	s := strings.NewReplacer(
		"{{ RATE_PER_SEC }}", strconv.FormatFloat(opts.LimiterOpts.RatePerSec, 'f', -1, 64),
		"{{ BUCKET_CAPACITY }}", strconv.Itoa(opts.LimiterOpts.Capacity),
	).Replace(luaTokenBucketScript)
	lua := rueidis.NewLuaScript(s)

	return &RedisKeyedLimiter{
		client: client,
		opts:   opts,
		script: lua,
	}, nil
}

func (l *RedisKeyedLimiter) Replenish(ctx context.Context, key string) error {
	return l.ReplenishN(ctx, key, 1)
}

func (l *RedisKeyedLimiter) Allow(ctx context.Context, key string) (ok bool, wait time.Duration, err error) {
	return l.AllowN(ctx, key, 1)
}

func (l *RedisKeyedLimiter) ReplenishN(ctx context.Context, key string, n int) error {
	_, _, err := l.AllowN(ctx, key, -n)
	return err
}

func (l *RedisKeyedLimiter) AllowN(ctx context.Context, key string, n int) (ok bool, wait time.Duration, err error) {
	if n > l.opts.LimiterOpts.Capacity {
		err = fmt.Errorf("%w: n (%d) > capacity (%d)", ErrExceedsBucketCapacity, n, l.opts.LimiterOpts.Capacity)
		return
	}

	resp := l.script.Exec(
		ctx,
		l.client,
		[]string{
			l.opts.RedisKey(key), // lua: state_key
		},
		[]string{
			strconv.Itoa(-n), // lua: tokens_delta (acquiring 1 = delta of -1)
		},
	)

	// Redis lua script should respond with array of 2 ints
	var ints []int64
	ints, err = resp.AsIntSlice()
	if err != nil {
		err = fmt.Errorf("acquiring limiter key %s: %w", key, err)
		return
	}
	if len(ints) != 2 {
		err = fmt.Errorf("incorrect result length: %d", len(ints))
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

// RedisCacheableKeyedLimiter is a non-replenishable limiter that caches wait durations to avoid
// unnecessary redis calls.
//
// If an earlier call to Allow was unsuccessful due to an empty bucket, it returns the wait time until the next
// token will be available. The limiter won't make further round-trips to redis until the wait time has elapsed.
type RedisCacheableKeyedLimiter struct {
	replenishable *RedisKeyedLimiter
	waitCache     *ttlcache.Cache[string, struct{}]

	// Currently just for tests
	cacheHits   atomic.Int64
	cacheMisses atomic.Int64
}

// NewRedisCacheableKeyedLimiter instantiates the limiter and starts a goroutine to clean up expired wait cache items.
// Call Stop when done.
func NewRedisCacheableKeyedLimiter(client rueidis.Client, opts RedisKeyedLimiterOpts) (*RedisCacheableKeyedLimiter, error) {
	rl, err := NewRedisKeyedLimiter(client, opts)
	if err != nil {
		return nil, err
	}

	// waitCache uses ttlcache as a self-cleaning map of [string]time.Time, by stuffing an empty struct into
	// the map value and treating the TTL/key expiry as the real value.
	//
	// keys stay in the map up until their ttl expires.
	waitCache := ttlcache.New(ttlcache.WithDisableTouchOnHit[string, struct{}]())
	go waitCache.Start()

	return &RedisCacheableKeyedLimiter{
		replenishable: rl,
		waitCache:     waitCache,
	}, nil
}

// Stop stops automatically cleaning up the internal wait cache
// Call when no longer using KeyedLimiter to avoid a goroutine leak.
func (l *RedisCacheableKeyedLimiter) Stop() {
	l.waitCache.Stop()
}

func (l *RedisCacheableKeyedLimiter) Allow(ctx context.Context, key string) (ok bool, wait time.Duration, err error) {
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

type RedisLimiterOpts struct {
	LimiterOpts LimiterOpts

	// RedisKey is the Redis key where the limiter state should be stored,
	// e.g. limiter:{my_global_action}
	RedisKey string
}

func (o *RedisLimiterOpts) Sanitize() error {
	if o.RedisKey == "" {
		return fmt.Errorf("RedisKey is required")
	}
	err := o.LimiterOpts.Sanitize()
	if err != nil {
		return fmt.Errorf("LimiterOpts: %w", err)
	}
	return nil
}

// RedisLimiter is a convenience wrapper around a RedisKeyedLimiter that limits a single default key
type RedisLimiter struct {
	source *RedisKeyedLimiter
}

func NewRedisLimiter(client rueidis.Client, opts RedisLimiterOpts) (*RedisLimiter, error) {
	err := opts.Sanitize()
	if err != nil {
		return nil, fmt.Errorf("opts: %w", err)
	}

	var l *RedisKeyedLimiter
	l, err = NewRedisKeyedLimiter(client, RedisKeyedLimiterOpts{
		LimiterOpts: opts.LimiterOpts,
		RedisKey: func(_ string) string {
			return opts.RedisKey
		},
	})
	if err != nil {
		return nil, fmt.Errorf("instantiating underlying keyed limiter: %w", err)
	}

	return &RedisLimiter{source: l}, nil
}

// NewRedisLimiterWithDefaultKey is a convenience initializer for an
// unkeyed RedisLimiter that writes to Redis keys "limiter:{ID}"
func NewRedisLimiterWithDefaultKey(
	client rueidis.Client,
	id string,
	opts LimiterOpts,
) (*RedisLimiter, error) {
	return NewRedisLimiter(client, RedisLimiterOpts{
		LimiterOpts: opts,
		RedisKey:    "limiter:{" + id + "}",
	})
}

func (l *RedisLimiter) Allow(ctx context.Context) (ok bool, wait time.Duration, err error) {
	return l.source.Allow(ctx, "")
}

func (l *RedisLimiter) AllowN(ctx context.Context, n int) (ok bool, wait time.Duration, err error) {
	return l.source.AllowN(ctx, "", n)
}

func (l *RedisLimiter) Replenish(ctx context.Context) error {
	return l.source.Replenish(ctx, "")
}

func (l *RedisLimiter) ReplenishN(ctx context.Context, n int) error {
	return l.source.ReplenishN(ctx, "", n)
}

// RedisCacheableLimiter is a convenience wrapper around a RedisCacheableKeyedLimiter that limits a single default key
// Caches wait times when the limiter is exhausted.
type RedisCacheableLimiter struct {
	source *RedisCacheableKeyedLimiter
}

func NewRedisCacheableLimiter(client rueidis.Client, opts RedisLimiterOpts) (*RedisCacheableLimiter, error) {
	err := opts.Sanitize()
	if err != nil {
		return nil, fmt.Errorf("opts: %w", err)
	}

	var l *RedisCacheableKeyedLimiter
	l, err = NewRedisCacheableKeyedLimiter(client, RedisKeyedLimiterOpts{
		LimiterOpts: opts.LimiterOpts,
		RedisKey: func(_ string) string {
			return opts.RedisKey
		},
	})
	if err != nil {
		return nil, fmt.Errorf("instantiating underlying keyed limiter: %w", err)
	}

	return &RedisCacheableLimiter{source: l}, nil
}

// NewRedisCacheableLimiterWithDefaultKey is a convenience initializer for an
// unkeyed RedisCacheableLimiter that writes to Redis keys "limiter:{ID}"
func NewRedisCacheableLimiterWithDefaultKey(
	client rueidis.Client,
	id string,
	opts LimiterOpts,
) (*RedisCacheableLimiter, error) {
	return NewRedisCacheableLimiter(client, RedisLimiterOpts{
		LimiterOpts: opts,
		RedisKey:    "limiter:{" + id + "}",
	})
}

func (l *RedisCacheableLimiter) Allow(ctx context.Context) (ok bool, wait time.Duration, err error) {
	return l.source.Allow(ctx, "")
}

//go:embed token_bucket.lua
var luaTokenBucketScript string

// ErrExceedsBucketCapacity means the number of tokens requested from the limiter exceeds the capacity of the
// bucket and therefore can never be satisfied
var ErrExceedsBucketCapacity = errors.New("request exceeds bucket capacity")
