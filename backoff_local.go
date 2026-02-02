package ruebucket

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/jellydator/ttlcache/v3"
)

// LocalKeyedBackoffLimiter is designed to mimic the redis-powered RedisKeyedBackoffLimiter,
// but implements its algorithm locally in Go.
type LocalKeyedBackoffLimiter struct {
	limiters *ttlcache.Cache[string, *LocalBackoffLimiter]
}

func NewLocalKeyedBackoffLimiter(opts BackoffOpts) (*LocalKeyedBackoffLimiter, error) {
	// Pre-sanitize opts, guarantee that future instantiations won't fail
	err := opts.Sanitize()
	if err != nil {
		return nil, fmt.Errorf("opts: %w", err)
	}

	// Lazily create local limiters when needed
	// Using suppressed (single flight) loader ensures that there is never more than one
	// limiter instance per key.
	singleFlightLoader := ttlcache.NewSuppressedLoader(ttlcache.LoaderFunc[string, *LocalBackoffLimiter](
		func(
			limiters *ttlcache.Cache[string, *LocalBackoffLimiter], key string,
		) *ttlcache.Item[string, *LocalBackoffLimiter] {
			l, lErr := NewLocalBackoffLimiter(&opts)
			if lErr != nil {
				panic("NewLocalBackoffLimiter failed with pre-sanitized opts: " + lErr.Error())
			}

			// Initial ttl set to time it takes for 1 penalty to expire.
			// Minimum amount of time the limiter could be relevant, assuming its used.
			return limiters.Set(key, l, opts.PenaltyDecayInterval)
		},
	), nil)

	limiters := ttlcache.New[string, *LocalBackoffLimiter](
		ttlcache.WithLoader(singleFlightLoader),
		ttlcache.WithDisableTouchOnHit[string, *LocalBackoffLimiter](), // No ttl magic, only change explicitly
	)

	// Purge expired limiters to prevent memory leak
	go limiters.Start()

	return &LocalKeyedBackoffLimiter{limiters: limiters}, nil
}

// Stop stops automatically cleaning up the limiter map
// Call when no longer using LocalKeyedBackoffLimiter to avoid a goroutine leak.
func (l *LocalKeyedBackoffLimiter) Stop() {
	l.limiters.Stop()
}

func (l *LocalKeyedBackoffLimiter) Reset(ctx context.Context, key string) error {
	// Deleting the limiter has the same effect as resetting it.
	// A new, default one will be allocated when next needed.
	l.limiters.Delete(key)
	return nil
}

func (l *LocalKeyedBackoffLimiter) Allow(ctx context.Context, key string) (ok bool, wait time.Duration, err error) {
	entry := l.limiters.Get(key)

	// TODO: Update LocalKeyedLimiter to do the same here rather than reaching in to grab mutex
	limiter := entry.Value()
	ok, wait = limiter.allow(ctx, func(newTTL time.Duration) {
		l.limiters.Set(key, limiter, newTTL)
	})

	return
}

// LocalBackoffLimiter implements a single in-memory backoff limiter, mimicking the
// Redis-powered BackoffLimiter.
type LocalBackoffLimiter struct {
	opts *BackoffOpts

	mu             sync.Mutex
	penalty        float64
	lastAcquiredAt time.Time
}

func NewLocalBackoffLimiter(opts *BackoffOpts) (*LocalBackoffLimiter, error) {
	// opts is pointer to allow common opts to be shared across multiple instances
	// held in LocalKeyedBackoffLimiter
	err := opts.Sanitize()
	if err != nil {
		return nil, fmt.Errorf("opts: %w", err)
	}

	return &LocalBackoffLimiter{opts: opts}, nil
}

func (l *LocalBackoffLimiter) Reset(_ context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.penalty = 0
	l.lastAcquiredAt = time.Time{}

	return nil
}

func (l *LocalBackoffLimiter) Allow(ctx context.Context) (ok bool, wait time.Duration, err error) {
	ok, wait = l.allow(ctx, func(time.Duration) {})
	return
}

func (l *LocalBackoffLimiter) allow(
	ctx context.Context,
	onTTLChange func(newTTL time.Duration),
) (ok bool, wait time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Time can be injected for testing
	now := time.Now()
	if t := injectedTimeFromContext(ctx); t != nil {
		now = *t
	}

	// Calculate time since the last successful acquisition.
	// Safety: clamp to 0 to prevent "penalty growth" if the system clock drifts backwards,
	// which would cause elapsed to go negative.
	elapsed := max(0, now.Sub(l.lastAcquiredAt))

	// Apply linear decay to the penalty based on how much time has passed
	// Don't allow penalty to go negative
	if !l.lastAcquiredAt.IsZero() {
		penaltyDecay := float64(elapsed) / float64(l.opts.PenaltyDecayInterval)
		l.penalty = max(0, l.penalty-penaltyDecay)
	}

	// Given the current penalty, how long until the next event is allowed
	var backoff time.Duration
	if l.penalty > 0 {
		// Note: X ** 0 = 1, so the penalty > 0 check ensures that a zero penalty
		// results in no wait, rather than erroneously defaulting to the BaseWait.
		//
		// We use max(0, l.penalty-1) as the exponent to ensure the first penalty,
		// e.g. 1, is not scaled by GrowthFactor.
		exp := max(0, l.penalty-1)
		backoff = min(
			time.Duration(float64(l.opts.BaseWait)*math.Pow(l.opts.GrowthFactor, exp)),
			l.opts.MaxWait,
		)
	}

	// Return remaining wait time if the backoff period hasn't fully elapsed.
	wait = backoff - elapsed
	if wait > 0 {
		return
	}

	// Successful acquisition: increment penalty and update state.
	// Clamp penalty to maxPenalty (derived from MaxWait) so that the
	// "internal debt" never exceeds the "external wait" ceiling.
	l.penalty = min(l.penalty+1, l.opts.maxPenalty)
	l.lastAcquiredAt = now

	// Calculate TTL: how long until the current penalty decays to zero.
	// This allows external caches (like ttlcache) to expire the record accurately.
	// Callback invoked while l.mu is held, ensures no races with concurrent calls
	ttl := time.Duration(l.penalty * float64(l.opts.PenaltyDecayInterval))
	onTTLChange(ttl)

	ok = true
	wait = 0

	return
}
