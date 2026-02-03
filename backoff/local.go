package backoff

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/jellydator/ttlcache/v3"
)

// LocalKeyedLimiter is designed to mimic the redis-powered RedisKeyedLimiter,
// but implements its algorithm locally in Go.
type LocalKeyedLimiter struct {
	limiters *ttlcache.Cache[string, *LocalLimiter]
}

func NewLocalKeyedLimiter(opts LimiterOpts) (*LocalKeyedLimiter, error) {
	// Pre-sanitize opts, guarantee that future instantiations won't fail
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

			// Initial ttl set to time it takes for 1 penalty to expire.
			// Minimum amount of time the limiter could be relevant, assuming its used.
			return limiters.Set(key, l, opts.PenaltyDecayInterval)
		},
	), nil)

	limiters := ttlcache.New[string, *LocalLimiter](
		ttlcache.WithLoader(singleFlightLoader),
		ttlcache.WithDisableTouchOnHit[string, *LocalLimiter](), // No ttl magic, only change explicitly
	)

	// Purge expired limiters to prevent memory leak
	go limiters.Start()

	return &LocalKeyedLimiter{limiters: limiters}, nil
}

// Stop stops automatically cleaning up the limiter map
// Call when no longer using LocalKeyedLimiter to avoid a goroutine leak.
func (l *LocalKeyedLimiter) Stop() {
	l.limiters.Stop()
}

func (l *LocalKeyedLimiter) Reset(ctx context.Context, key string) error {
	// Deleting the limiter has the same effect as resetting it.
	// A new, default one will be allocated when next needed.
	// TODO: What happens if Delete is called during a call to Allow?
	l.limiters.Delete(key)
	return nil
}

func (l *LocalKeyedLimiter) Allow(ctx context.Context, key string) (ok bool, wait time.Duration, err error) {
	entry := l.limiters.Get(key)

	limiter := entry.Value()
	ok, wait = limiter.allow(ctx, func(newTTL time.Duration) {
		l.limiters.Set(key, limiter, newTTL)
	})

	return
}

// LocalLimiter implements a single in-memory backoff limiter, mimicking the
// Redis-powered BackoffLimiter.
type LocalLimiter struct {
	opts *LimiterOpts

	mu             sync.Mutex
	penalty        float64
	lastAcquiredAt time.Time
}

func NewLocalLimiter(opts *LimiterOpts) (*LocalLimiter, error) {
	// opts is pointer to allow common opts to be shared across multiple instances
	// held in LocalKeyedBackoffLimiter
	err := opts.Sanitize()
	if err != nil {
		return nil, fmt.Errorf("opts: %w", err)
	}

	return &LocalLimiter{opts: opts}, nil
}

func (l *LocalLimiter) Reset(_ context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.penalty = 0
	l.lastAcquiredAt = time.Time{}

	return nil
}

func (l *LocalLimiter) Allow(ctx context.Context) (ok bool, wait time.Duration, err error) {
	ok, wait = l.allow(ctx, func(time.Duration) {})
	return
}

func (l *LocalLimiter) allow(
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
