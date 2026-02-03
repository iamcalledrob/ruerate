package tokenbucket

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jellydator/ttlcache/v3"
)

// TODO: Merge localTokenBucket w/ LocalLimiter, and make LocalLimiter take *opts

// Local token bucket limiters that match behaviour of the replenishable redis counterparts.
// Implemented to make it easy to use shared (redis-backed) limiters together with local (in memory) limiters.

// LocalKeyedLimiter manages in-memory token buckets and uses the same semantics as the redis-backed
// ReplenishableKeyedLimiter.
//
// As with redis KeyedLimiter, only non-full buckets are kept in memory, and a goroutine is automatically
// started to purge expired buckets.
type LocalKeyedLimiter struct {
	opts    LimiterOpts
	buckets *ttlcache.Cache[string, *localTokenBucket]
}

func NewLocalKeyedLimiter(opts LimiterOpts) (*LocalKeyedLimiter, error) {
	err := opts.Sanitize()
	if err != nil {
		return nil, fmt.Errorf("opts: %w", err)
	}

	// Lazily create token buckets when needed
	// Using suppressed (single flight) loader ensures that there is never more than one bucket instance
	// per key.
	singleFlightLoader := ttlcache.NewSuppressedLoader(ttlcache.LoaderFunc[string, *localTokenBucket](
		func(
			buckets *ttlcache.Cache[string, *localTokenBucket], key string,
		) *ttlcache.Item[string, *localTokenBucket] {
			bucket := newLocalTokenBucket(opts.RatePerSec, opts.Capacity)
			ttl := time.Until(bucket.fullAt())
			return buckets.Set(key, bucket, ttl)
		},
	), nil)

	buckets := ttlcache.New[string, *localTokenBucket](
		ttlcache.WithLoader(singleFlightLoader),
		ttlcache.WithDisableTouchOnHit[string, *localTokenBucket](), // No ttl magic, only change explicitly
	)

	// Purge expired buckets to prevent memory leak
	go buckets.Start()

	return &LocalKeyedLimiter{opts: opts, buckets: buckets}, nil
}

// Stop stops automatically cleaning up the limiter map
// Call when no longer using LocalKeyedLimiter to avoid a goroutine leak.
func (l *LocalKeyedLimiter) Stop() {
	l.buckets.Stop()
}

func (l *LocalKeyedLimiter) Replenish(ctx context.Context, key string) error {
	return l.ReplenishN(ctx, key, 1)
}

func (l *LocalKeyedLimiter) Allow(ctx context.Context, key string) (ok bool, wait time.Duration, err error) {
	return l.AllowN(ctx, key, 1)
}

func (l *LocalKeyedLimiter) ReplenishN(ctx context.Context, key string, n int) error {
	_, _, err := l.AllowN(ctx, key, -n)
	return err
}

func (l *LocalKeyedLimiter) AllowN(_ context.Context, key string, n int) (ok bool, wait time.Duration, err error) {
	if n > l.opts.Capacity {
		err = fmt.Errorf("%w: n (%d) > capacity (%d)", ErrExceedsBucketCapacity, n, l.opts.Capacity)
		return
	}

	// Auto-instantiated if not yet set, so should never return nil
	entry := l.buckets.Get(key)
	bucket := entry.Value()

	// By locking the bucket and holding until return, we can ensure that the Set below is not subject to
	// a race condition where
	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	ok, wait = bucket.update(float64(-n))

	// Not enough tokens: limiter state unchanged
	if !ok {
		return
	}

	// Token acquired, limiter state mutated
	// 'Set' to update ttl so bucket auto-expires when it will now become full
	l.buckets.Set(key, bucket, time.Until(bucket.fullAt()))
	return
}

// LocalLimiter is an in-memory token bucket that uses the same semantics as the redis-backed Limiter
type LocalLimiter struct {
	bucket *localTokenBucket
}

func NewLocalLimiter(opts LimiterOpts) (*LocalLimiter, error) {
	err := opts.Sanitize()
	if err != nil {
		return nil, fmt.Errorf("opts: %w", err)
	}
	return &LocalLimiter{bucket: newLocalTokenBucket(opts.RatePerSec, opts.Capacity)}, nil
}

func (l *LocalLimiter) Replenish(ctx context.Context) error {
	return l.ReplenishN(ctx, 1)
}

func (l *LocalLimiter) Allow(ctx context.Context) (ok bool, wait time.Duration, err error) {
	ok, wait, err = l.AllowN(ctx, 1)
	return
}

func (l *LocalLimiter) ReplenishN(ctx context.Context, n int) error {
	_, _, err := l.AllowN(ctx, -n)
	return err
}

func (l *LocalLimiter) AllowN(_ context.Context, n int) (ok bool, wait time.Duration, err error) {
	if n > l.bucket.bucketCapacity {
		err = fmt.Errorf("%w: n (%d) > capacity (%d)", ErrExceedsBucketCapacity, n, l.bucket.bucketCapacity)
		return
	}

	l.bucket.mu.Lock()
	defer l.bucket.mu.Unlock()
	ok, wait = l.bucket.update(float64(-n))
	return
}

// localTokenBucket is designed to mimic the redis token bucket implemented in limiter_token_bucket.lua
//
// It's expected that the caller manages the lock, in order to allow update() + fullAt() to be invoked without
// the possibility of interleaving.
type localTokenBucket struct {
	ratePerSec     float64
	bucketCapacity int

	mu             sync.Mutex
	lastTokens     float64
	lastAcquiredAt time.Time
}

func newLocalTokenBucket(ratePerSec float64, bucketCapacity int) *localTokenBucket {
	return &localTokenBucket{
		ratePerSec:     ratePerSec,
		bucketCapacity: bucketCapacity,
		lastTokens:     float64(bucketCapacity),
	}
}

func (b *localTokenBucket) update(tokensDelta float64) (ok bool, wait time.Duration) {
	now := time.Now()

	// Calculate how many tokens the bucket would have after applying accrued tokens
	// and the requested delta. Constrain to bucket capacity.
	currentTokens := min(b.lastTokens+b.accruedTokens(now)+tokensDelta, float64(b.bucketCapacity))

	// Acquisition would take tokens negative: not allowed
	if currentTokens < 0 {
		shortfall := 0 - currentTokens
		waitSecs := shortfall / b.ratePerSec
		wait = time.Duration(waitSecs * float64(time.Second))
		return false, wait
	}

	// Acquisition is successful
	b.lastTokens = currentTokens
	b.lastAcquiredAt = now
	return true, 0
}

// Calculate how many tokens have accrued as a result of the passage of time
func (b *localTokenBucket) accruedTokens(now time.Time) float64 {
	if b.lastAcquiredAt.IsZero() {
		return 0
	}

	timeDeltaSecs := now.Sub(b.lastAcquiredAt).Seconds()
	return timeDeltaSecs * b.ratePerSec
}

// Calculate at what time the bucket will be completely full
func (b *localTokenBucket) fullAt() time.Time {
	now := time.Now()

	tokensUntilFull := float64(b.bucketCapacity) - b.lastTokens - b.accruedTokens(now)
	if tokensUntilFull <= 0 {
		// Already full
		return now
	}

	secsUntilFull := tokensUntilFull / b.ratePerSec
	return now.Add(time.Duration(secsUntilFull * float64(time.Second)))
}
