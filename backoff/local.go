package backoff

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/iamcalledrob/ruebucket/internal/safetime"
	"github.com/iamcalledrob/ruebucket/internal/ttlmap"
)

// LocalKeyedLimiter is designed to mimic the redis-powered RedisKeyedLimiter,
// but implements its algorithm locally in Go.
type LocalKeyedLimiter struct {
	opts     LimiterOpts
	limiters *ttlmap.Map[string, *LocalLimiter]
}

func NewLocalKeyedLimiter(opts LimiterOpts) (*LocalKeyedLimiter, error) {
	// Pre-sanitize opts, guarantee that future instantiations won't fail
	err := opts.Sanitize()
	if err != nil {
		return nil, fmt.Errorf("opts: %w", err)
	}

	return &LocalKeyedLimiter{
		opts:     opts,
		limiters: ttlmap.New[string, *LocalLimiter](),
	}, nil
}

func (l *LocalKeyedLimiter) Reset(ctx context.Context, key string) error {
	shard := l.limiters.Shard(key)
	shard.Lock()
	defer shard.Unlock()

	// Deleting the limiter has the same effect as resetting it.
	// A new, default one will be allocated when next needed.
	shard.Delete(key)
	return nil
}

func (l *LocalKeyedLimiter) Allow(ctx context.Context, key string) (ok bool, wait time.Duration, err error) {
	// Lock only this key's shard rather than the entire keyed limiter
	var shard *ttlmap.Shard[string, *LocalLimiter]
	shard = l.limiters.Shard(key)
	shard.Lock()
	defer shard.Unlock()

	// Propagate injected time into ttlmap shard, otherwise
	// keys may expire prematurely
	if isTesting {
		if t := injectedTimeFromContext(ctx); t != nil {
			shard.SetNow(t)
			defer shard.SetNow(nil)
		}
	}

	var limiter *LocalLimiter
	limiter, _, ok = shard.Get(key)
	if !ok {
		limiter, err = NewLocalLimiter(&l.opts)
		if err != nil {
			panic("NewLocalLimiter failed with pre-sanitized opts: " + err.Error())
		}
	}

	var decayedAt time.Time
	ok, wait, decayedAt = limiter.allowLocked(ctx)
	// If not allowed, the limiter will not have been mutated
	if !ok {
		return
	}

	// Set or replace (if already exists) existing map key
	shard.Set(key, limiter, decayedAt)

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
	l.resetLocked()
	return nil
}

func (l *LocalLimiter) resetLocked() {
	l.penalty = 0
	l.lastAcquiredAt = time.Time{}
}

func (l *LocalLimiter) Allow(ctx context.Context) (ok bool, wait time.Duration, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	ok, wait, _ = l.allowLocked(ctx)
	return
}

func (l *LocalLimiter) allowLocked(ctx context.Context) (ok bool, wait time.Duration, decayedAt time.Time) {
	// Time can be injected for testing
	now := time.Now()
	if isTesting {
		if t := injectedTimeFromContext(ctx); t != nil {
			now = *t
		}
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
		pow := math.Pow(l.opts.GrowthFactor, exp)
		backoff = min(
			safetime.Duration(float64(l.opts.BaseWait)*pow),
			l.opts.MaxWait,
			l.opts.PenaltyDecayInterval,
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

	ok = true
	wait = 0

	// How long until the current penalty decays to zero.
	// This is needed for external caches (like TTLMap) to expire the record accurately.
	// safetime bounds checking
	ttl := safetime.Duration(l.penalty * float64(l.opts.PenaltyDecayInterval))
	decayedAt = safetime.Add(now, ttl)
	return
}

var isTesting bool
