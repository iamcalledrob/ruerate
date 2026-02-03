package ruerate

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLocalBackoffLimiter(t *testing.T) {
	testBackoffLimiter_Common(t, func(opts BackoffOpts) (testableLimiter, error) {
		return NewLocalBackoffLimiter(&opts)
	})
}

func TestLocalKeyedBackoffLimiter(t *testing.T) {
	testBackoffLimiter_Common(t, func(opts BackoffOpts) (testableLimiter, error) {
		kl, err := NewLocalKeyedBackoffLimiter(opts)
		if err != nil {
			return nil, fmt.Errorf("instantiating local keyed backoff limiter: %w", err)
		}
		return &singleKeyLocalBackoffLimiter{source: kl}, nil
	})

	t.Run("CacheKeyExpiry", func(t *testing.T) {
		const key = "KEY"

		// Ensure cache properly expires items when penalty has fully decayed
		t.Run("PenaltyDecay", func(t *testing.T) {
			kl, err := NewLocalKeyedBackoffLimiter(BackoffOpts{
				BaseWait:             100 * time.Millisecond,
				PenaltyDecayInterval: 100 * time.Millisecond,
			})
			require.NoError(t, err)

			var ok bool
			ok, _, err = kl.Allow(t.Context(), key)
			require.NoError(t, err)
			require.True(t, ok)

			// Check that an entry exists in the cache
			// Ensure its TTL matches when the penalty would have fully decayed
			// Allow 5% slop to avoid races with slow test runners etc.
			entry := kl.limiters.Get("key")
			require.NotNil(t, entry)

			// Ensure TTL is set as expected
			require.InDelta(t, 100*time.Millisecond, entry.TTL(), float64(5*time.Millisecond))

			// Ensure item does get removed after TTL expires
			<-time.After(100 * time.Millisecond)
			exists := kl.limiters.Has(key)
			require.False(t, exists)
		})

		// Ensure cache properly expires items when reset is invoked
		t.Run("Reset", func(t *testing.T) {
			kl, err := NewLocalKeyedBackoffLimiter(BackoffOpts{
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

			exists := kl.limiters.Has(key)
			require.False(t, exists)
		})
	})

	// TODO: Visualize scenario with a min reconnect interval. If not as a test, then just as a util func.
}

// Shim to allow testing of a local keyed limiter as a regular limiter
type singleKeyLocalBackoffLimiter struct {
	source *LocalKeyedBackoffLimiter
}

func (l *singleKeyLocalBackoffLimiter) Reset(ctx context.Context) error {
	return l.source.Reset(ctx, "default")
}

func (l *singleKeyLocalBackoffLimiter) Allow(ctx context.Context) (ok bool, wait time.Duration, err error) {
	ok, wait, err = l.source.Allow(ctx, "default")
	return
}
