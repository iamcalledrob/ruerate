package tokenbucket

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/iamcalledrob/ruebucket/internal/safetime"
	"github.com/iamcalledrob/ruebucket/internal/ttlmap"
)

// Local token bucket limiters that match behaviour of the replenishable redis counterparts.
// Implemented to make it easy to use shared (redis-backed) limiters together with local (in memory) limiters.

// LocalKeyedLimiter manages in-memory token buckets and uses the same semantics as the redis-backed
// ReplenishableKeyedLimiter.
//
// As with redis KeyedLimiter, only non-full buckets are kept in memory
type LocalKeyedLimiter struct {
	opts     LimiterOpts
	limiters *ttlmap.Map[string, *LocalLimiter]
}

func NewLocalKeyedLimiter(opts LimiterOpts) (*LocalKeyedLimiter, error) {
	err := opts.Sanitize()
	if err != nil {
		return nil, fmt.Errorf("opts: %w", err)
	}

	return &LocalKeyedLimiter{
		opts:     opts,
		limiters: ttlmap.New[string, *LocalLimiter](),
	}, nil
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
	var shard *ttlmap.Shard[string, *LocalLimiter]
	shard = l.limiters.Shard(key)
	shard.Lock()
	defer shard.Unlock()

	var limiter *LocalLimiter
	limiter, ok = shard.GetValue(key)
	if !ok {
		limiter, err = NewLocalLimiter(&l.opts)
		if err != nil {
			panic("NewLocalLimiter failed with pre-sanitized opts: " + err.Error())
		}
	}

	// Can invoke updateLocked directly, entire shard is locked.
	// Avoids double-lock
	var fullAt time.Time
	ok, wait, fullAt, err = limiter.updateLocked(-n)

	// Map entry expires when bucket is full
	shard.Set(key, limiter, fullAt)
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

func (l *LocalLimiter) ReplenishN(ctx context.Context, n int) error {
	_, _, err := l.AllowN(ctx, -n)
	return err
}

func (l *LocalLimiter) Allow(ctx context.Context) (ok bool, wait time.Duration, err error) {
	ok, wait, err = l.AllowN(ctx, 1)
	return
}

func (l *LocalLimiter) AllowN(_ context.Context, n int) (ok bool, wait time.Duration, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	ok, wait, _, err = l.updateLocked(-n)
	return
}

func (l *LocalLimiter) updateLocked(tokensDelta int) (ok bool, wait time.Duration, fullAt time.Time, err error) {
	if -tokensDelta > l.opts.Capacity {
		err = fmt.Errorf("%w: tokensDelta (%d) > capacity (%d)", ErrExceedsBucketCapacity, tokensDelta, l.opts.Capacity)
		return
	}
	now := time.Now()

	// Calculate how many tokens the bucket would have after applying accrued tokens
	// and the requested delta. Constrain to bucket capacity.
	var accruedTokens float64
	if !l.lastAcquiredAt.IsZero() {
		elapsed := max(0, now.Sub(l.lastAcquiredAt))
		accruedTokens = l.opts.RatePerSec * elapsed.Seconds()
	}

	currentTokens := min(
		l.lastTokens+accruedTokens+float64(tokensDelta),
		float64(l.opts.Capacity),
	)

	// Acquisition would take tokens negative: not allowed
	if currentTokens < 0 {
		shortfall := 0 - currentTokens
		waitSecs := shortfall / l.opts.RatePerSec
		wait = safetime.Duration(waitSecs * float64(time.Second))
		return
	}

	// Acquisition is successful
	l.lastTokens = currentTokens
	l.lastAcquiredAt = now

	ok = true
	wait = 0

	tokensAcquired := float64(l.opts.Capacity) - l.lastTokens
	secsUntilFull := tokensAcquired / l.opts.RatePerSec
	fullAt = safetime.Add(now, safetime.Duration(secsUntilFull*float64(time.Second)))

	return
}
