package tokenbucket

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jellydator/ttlcache/v3"
)

// Local token bucket limiters that match behaviour of the replenishable redis counterparts.
// Implemented to make it easy to use shared (redis-backed) limiters together with local (in memory) limiters.

// LocalKeyedLimiter manages in-memory token buckets and uses the same semantics as the redis-backed
// ReplenishableKeyedLimiter.
//
// As with redis KeyedLimiter, only non-full buckets are kept in memory, and a goroutine is automatically
// started to purge expired buckets.
type LocalKeyedLimiter struct {
	opts     LimiterOpts
	limiters *ttlcache.Cache[string, *LocalLimiter]
}

func NewLocalKeyedLimiter(opts LimiterOpts) (*LocalKeyedLimiter, error) {
	err := opts.Sanitize()
	if err != nil {
		return nil, fmt.Errorf("opts: %w", err)
	}

	// Lazily create local limiters when needed
	// Using suppressed (single flight) loader ensures that there is never more than one
	// limiter instance per key.
	singleFlightLoader := ttlcache.NewSuppressedLoader(ttlcache.LoaderFunc[string, *LocalLimiter](
		func(
			limiters *ttlcache.Cache[string, *LocalLimiter], key string,
		) *ttlcache.Item[string, *LocalLimiter] {
			l, lErr := NewLocalLimiter(&opts)
			if lErr != nil {
				panic("NewLocalLimiter failed with pre-sanitized opts: " + lErr.Error())
			}

			// Set a short, arbitrary initial TTL so the bucket doesn't get cleared from the cache
			// TODO: Is this safe/correct?
			ttl := 10 * time.Millisecond
			return limiters.Set(key, l, ttl)
		},
	), nil)

	limiters := ttlcache.New[string, *LocalLimiter](
		ttlcache.WithLoader(singleFlightLoader),
		ttlcache.WithDisableTouchOnHit[string, *LocalLimiter](), // No ttl magic, only change explicitly
	)

	// Purge expired buckets to prevent memory leak
	go limiters.Start()

	return &LocalKeyedLimiter{opts: opts, limiters: limiters}, nil
}

// Stop stops automatically cleaning up the limiter map
// Call when no longer using LocalKeyedLimiter to avoid a goroutine leak.
func (l *LocalKeyedLimiter) Stop() {
	l.limiters.Stop()
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
	entry := l.limiters.Get(key)
	// TODO: Is this not a race condition? What if bucket gets cleared by its TTL here, and so we end up
	//       with 2x buckets "in flight"? As LocalKeyedLimiter doesn't serialize calls to AllowN etc.

	limiter := entry.Value()
	ok, wait, err = limiter.update(-n, func(newTTL time.Duration) {
		l.limiters.Set(key, limiter, newTTL)
	})

	return

}

// LocalLimiter is an in-memory token bucket that uses the same semantics as the redis-backed Limiter
type LocalLimiter struct {
	opts *LimiterOpts

	mu             sync.Mutex
	lastTokens     float64
	lastAcquiredAt time.Time
}

func NewLocalLimiter(opts *LimiterOpts) (*LocalLimiter, error) {
	err := opts.Sanitize()
	if err != nil {
		return nil, fmt.Errorf("opts: %w", err)
	}
	return &LocalLimiter{opts: opts, lastTokens: float64(opts.Capacity)}, nil
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
	return l.update(-n, nil)
}

func (l *LocalLimiter) update(
	tokensDelta int,
	onTTLChange func(newTTL time.Duration),
) (ok bool, wait time.Duration, err error) {
	if -tokensDelta > l.opts.Capacity {
		err = fmt.Errorf("%w: tokensDelta (%d) > capacity (%d)", ErrExceedsBucketCapacity, tokensDelta, l.opts.Capacity)
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()

	// Calculate how many tokens the bucket would have after applying accrued tokens
	// and the requested delta. Constrain to bucket capacity.
	var accruedTokens float64
	if !l.lastAcquiredAt.IsZero() {
		elapsed := max(0, now.Sub(l.lastAcquiredAt))
		accruedTokens = l.opts.RatePerSec * elapsed.Seconds()
	}

	currentTokens := min(l.lastTokens+accruedTokens+float64(tokensDelta), float64(l.opts.Capacity))

	// Acquisition would take tokens negative: not allowed
	if currentTokens < 0 {
		shortfall := 0 - currentTokens
		waitSecs := shortfall / l.opts.RatePerSec
		wait = time.Duration(waitSecs * float64(time.Second))
		return
	}

	// Acquisition is successful
	l.lastTokens = currentTokens
	l.lastAcquiredAt = now

	// TODO: Simplify, can probably tidy up along with currentTokens calculation
	if onTTLChange != nil {
		var ttl time.Duration
		tokensUntilFull := float64(l.opts.Capacity) - l.lastTokens - accruedTokens
		if tokensUntilFull > 0 {
			secsUntilFull := tokensUntilFull / l.opts.RatePerSec
			ttl = time.Duration(secsUntilFull * float64(time.Second))
		}
		onTTLChange(ttl)
	}

	ok = true
	wait = 0

	return
}
