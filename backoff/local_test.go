package backoff

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLocalLimiter(t *testing.T) {
	testLimiter_Common(t, func(opts LimiterOpts) (Limiter, error) {
		return NewLocalLimiter(&opts)
	})
}

func TestLocalKeyedLimiter(t *testing.T) {
	testLimiter_Common(t, func(opts LimiterOpts) (Limiter, error) {
		kl, err := NewLocalKeyedLimiter(opts)
		if err != nil {
			return nil, fmt.Errorf("instantiating local keyed backoff limiter: %w", err)
		}
		return &singleKeyLocalBackoffLimiter{source: kl}, nil
	})

	t.Run("CacheKeyExpiry", func(t *testing.T) {
		const key = "KEY"

		// Ensure cache properly expires items when penalty has fully decayed
		t.Run("PenaltyDecay", func(t *testing.T) {
			kl, err := NewLocalKeyedLimiter(LimiterOpts{
				BaseWait:             100 * time.Millisecond,
				PenaltyDecayInterval: 100 * time.Millisecond,
			})
			require.NoError(t, err)

			var ok bool
			ok, _, err = kl.Allow(t.Context(), key)
			require.NoError(t, err)
			require.True(t, ok)

			// Check that an entry exists in the cache
			// Ensure its expiry matches when the penalty would have fully decayed
			// Allow 5% slop to avoid races with slow test runners etc.

			_, expiresAt, ok := kl.limiters.Shard(key).Get(key)
			require.True(t, ok)

			// Ensure expiresAt is set as expected
			require.WithinDuration(t, time.Now().Add(100*time.Millisecond), expiresAt, 5*time.Millisecond)

			// Ensure item does get removed after TTL expires
			<-time.After(100 * time.Millisecond)
			_, exists := kl.limiters.Shard(key).GetValue(key)
			require.False(t, exists)
		})

		// Ensure cache properly expires items when reset is invoked
		t.Run("Reset", func(t *testing.T) {
			kl, err := NewLocalKeyedLimiter(LimiterOpts{
				BaseWait:             100 * time.Millisecond,
				PenaltyDecayInterval: 100 * time.Millisecond,
			})
			require.NoError(t, err)

			// Ensure key is created
			var ok bool
			ok, _, err = kl.Allow(t.Context(), key)
			require.NoError(t, err)
			require.True(t, ok)

			// Reset, should expire the key
			err = kl.Reset(t.Context(), key)
			require.NoError(t, err)

			_, exists := kl.limiters.Shard(key).GetValue(key)
			require.False(t, exists)
		})
	})
}

func BenchmarkLocalLimiter(b *testing.B) {
	benchmarkLimiter(b, func(opts LimiterOpts) (Limiter, error) {
		return NewLocalLimiter(&opts)
	})
}

func BenchmarkLocalKeyedLimiter(b *testing.B) {
	benchmarkLimiter(b, func(opts LimiterOpts) (Limiter, error) {
		l, err := NewLocalKeyedLimiter(opts)
		if err != nil {
			return nil, err
		}
		return &singleKeyLocalBackoffLimiter{source: l}, nil
	})
}

// Shim to allow testing of a local keyed limiter as a regular limiter
type singleKeyLocalBackoffLimiter struct {
	source *LocalKeyedLimiter
}

func (l *singleKeyLocalBackoffLimiter) Reset(ctx context.Context) error {
	return l.source.Reset(ctx, "default")
}

func (l *singleKeyLocalBackoffLimiter) Allow(ctx context.Context) (ok bool, wait time.Duration, err error) {
	ok, wait, err = l.source.Allow(ctx, "default")
	return
}
