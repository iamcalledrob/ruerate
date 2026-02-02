package ruebucket

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testableLimiter interface {
	Allow(context.Context) (bool, time.Duration, error)
	Reset(context.Context) error
}

// Common backoff tests
func testBackoffLimiter_Common(
	t *testing.T,
	factory func(opts BackoffOpts) (testableLimiter, error),
) {
	t.Run("Allow", func(t *testing.T) {
		testBackoffLimiter_Allow(t, factory)
	})
	t.Run("Reset", func(t *testing.T) {
		testBackoffLimiter_Reset(t, factory)
	})
	t.Run("AllowBeforeLast", func(t *testing.T) {
		testBackoffLimiter_AllowBeforeLast(t, factory)
	})
	t.Run("GrowthFactor", func(t *testing.T) {
		testBackoffLimiter_GrowthFactor(t, factory)
	})
	t.Run("MaxWait", func(t *testing.T) {
		testBackoffLimiter_MaxWait(t, factory)
	})
	t.Run("PrematureAttemptsDontGrow", func(t *testing.T) {
		testBackoffLimiter_PrematureAttemptsDontGrow(t, factory)
	})
	t.Run("WaitScalesWithDecay", func(t *testing.T) {
		testBackoffLimiter_WaitScalesWithDecay(t, factory)
	})
	t.Run("WaitExceedsBaselineBeforeDecay", func(t *testing.T) {
		testBackoffLimiter_WaitExceedsBaselineBeforeDecay(t, factory)
	})
	t.Run("DecayScenario", func(t *testing.T) {
		testBackoffLimiter_DecayScenario(t, factory)
	})
}

func testBackoffLimiter_Allow(
	t *testing.T,
	factory func(opts BackoffOpts) (testableLimiter, error),
) {
	t.Run("NeverInitialized", func(t *testing.T) {
		l, err := factory(BackoffOpts{})
		require.NoError(t, err)

		var ok bool
		var wait time.Duration
		ok, wait, err = l.Allow(t.Context())
		require.NoError(t, err)

		// Should be allowed with no wait, as limiter is in default state.
		require.True(t, ok)
		require.Equal(t, time.Duration(0), wait)
	})
	t.Run("WithPenalty", func(t *testing.T) {
		l, err := factory(BackoffOpts{})
		require.NoError(t, err)

		// Incur penalty
		var ok bool
		var wait time.Duration
		ok, wait, err = l.Allow(t.Context())
		require.NoError(t, err)
		require.True(t, ok)
		require.Zero(t, wait)

		// Subsequent immediate call should reflect penalty in place
		//(i.e. caller should be told to wait)
		ok, wait, err = l.Allow(t.Context())
		require.NoError(t, err)
		require.False(t, ok)
		require.NotZero(t, wait)
	})
	t.Run("WithFullyDecayedPenalty", func(t *testing.T) {
		decayTime := 100 * time.Millisecond
		l, err := factory(BackoffOpts{
			BaseWait:             50 * time.Millisecond,
			PenaltyDecayInterval: decayTime,
		})
		require.NoError(t, err)

		// Increment penalty by calling Allow successfully
		var ok bool
		var wait time.Duration
		ok, wait, err = l.Allow(t.Context())
		require.NoError(t, err)
		require.True(t, ok)
		require.Zero(t, wait)

		// Wait until penalty fully decayed
		// Using real clock as a more realistic scenario, there may
		// be bugs related to cache TTLs that only occur with a real clock
		<-time.After(decayTime)

		ok, wait, err = l.Allow(t.Context())
		require.NoError(t, err)
		require.True(t, ok)
		require.Zero(t, wait)
	})
}

func testBackoffLimiter_Reset(
	t *testing.T,
	factory func(opts BackoffOpts) (testableLimiter, error),
) {
	// Ensure Reset does not fail even if the limiter has never been seen before
	t.Run("NeverInitialized", func(t *testing.T) {
		l, err := factory(BackoffOpts{})
		require.NoError(t, err)

		err = l.Reset(t.Context())
		require.NoError(t, err)
	})

	// Ensure Reset works when the limiter has a penalty
	// i.e. should be kept track of
	t.Run("WithPenalty", func(t *testing.T) {
		// Ensure penalty won't fully decay during test
		l, err := factory(BackoffOpts{
			PenaltyDecayInterval: 24 * time.Hour,
		})
		require.NoError(t, err)

		// Increment penalty by calling Allow successfully
		var ok bool
		ok, _, err = l.Allow(t.Context())
		require.NoError(t, err)
		require.True(t, ok)

		// Immediately hit the limiter again, should return ok = false
		// due to existing penalty
		ok, _, err = l.Allow(t.Context())
		require.NoError(t, err)
		require.False(t, ok)

		// Invoke Reset, ensure no errors
		err = l.Reset(t.Context())
		require.NoError(t, err)

		// Hit limiter post-reset, should return ok = true as penalty
		// has been cleared
		ok, _, err = l.Allow(t.Context())
		require.NoError(t, err)
		require.True(t, ok)
	})

	// Ensures Reset doesn't fail when the limiter *had* a penalty,
	// but it has fully decayed.
	t.Run("WithFullyDecayedPenalty", func(t *testing.T) {
		decayTime := 100 * time.Millisecond
		l, err := factory(BackoffOpts{
			BaseWait:             50 * time.Millisecond,
			MaxWait:              75 * time.Millisecond,
			PenaltyDecayInterval: decayTime,
		})
		require.NoError(t, err)

		// Increment penalty by calling Allow successfully
		var ok bool
		ok, _, err = l.Allow(t.Context())
		require.NoError(t, err)
		require.True(t, ok)

		// Wait until penalty fully decayed
		// Using real clock as a more realistic scenario -- allows for
		// calling of Reset after expiry, whereas using withInjectedTime
		// might not be used by internal Reset mechanism.
		<-time.After(decayTime)

		// Reset, ensure no errors
		err = l.Reset(t.Context())
		require.NoError(t, err)

		// Hit limiter post-reset, should return ok = true as penalty
		// has been cleared. Ensures the reset didn't do anything stupid,
		// like cause a negative penalty.
		ok, _, err = l.Allow(t.Context())
		require.NoError(t, err)
		require.True(t, ok)
	})
}

// Ensure any internal clock changes don't totally break stuff, e.g. by
// applying negative penalties
func testBackoffLimiter_AllowBeforeLast(
	t *testing.T,
	factory func(opts BackoffOpts) (testableLimiter, error),
) {
	opts := BackoffOpts{
		BaseWait: 500 * time.Millisecond,
	}
	l, err := factory(opts)
	require.NoError(t, err)

	now := time.Now()
	ok, _, err := l.Allow(withInjectedTime(t.Context(), now))
	require.NoError(t, err)
	require.True(t, ok)

	// The clock skips backwards.
	now = time.Now().Add(-time.Hour)

	// The limiter should deny the next request, and should consider zero time
	// to have elapsed since the last, rather than doing any funky calculations
	// Before a fix was implemented, this could cause negative wait times etc.
	var wait time.Duration
	ok, wait, err = l.Allow(withInjectedTime(t.Context(), now))
	require.NoError(t, err)
	require.False(t, ok)

	// Should still have to wait, and since zero time has elapsed, wait is BaseWait.
	require.EqualValues(t, opts.BaseWait, wait)
}

// Ensures the base wait delay grows by the growthFactor
// Inherent in this is that the first delay = unscaled BaseWait
func testBackoffLimiter_GrowthFactor(
	t *testing.T,
	factory func(opts BackoffOpts) (testableLimiter, error),
) {
	// Not a sentinel, but expires about 1 penalty point per lifetime of the universe
	NoDecay := time.Duration(math.MaxInt64)

	t.Run("Base1sFactor1.2", func(t *testing.T) {
		want := []float64{1.0, 1.1, 1.21, 1.34, 1.47, 1.61, 1.77, 1.95, 2.14, 2.36}

		l, err := factory(BackoffOpts{
			BaseWait:             1 * time.Second,
			PenaltyDecayInterval: NoDecay,
			GrowthFactor:         1.1,
		})
		require.NoError(t, err)
		got, _ := hammerLimiter(t, time.Now(), l, len(want))
		require.InDeltaSlice(t, want, got, 0.01)
	})
	t.Run("Base1sFactor2", func(t *testing.T) {
		want := []float64{1, 2, 4, 8, 16, 32, 64, 128, 256}

		l, err := factory(BackoffOpts{
			BaseWait:             1 * time.Second,
			MaxWait:              99999 * time.Hour,
			PenaltyDecayInterval: NoDecay,
			GrowthFactor:         2,
		})
		require.NoError(t, err)
		got, _ := hammerLimiter(t, time.Now(), l, len(want))

		// Allow 0.01 slop
		require.InDeltaSlice(t, want, got, 0.01)
	})
	t.Run("Base1sFactor3", func(t *testing.T) {
		want := []float64{1, 3, 9, 27, 81, 243, 729, 2187, 6561}

		l, err := factory(BackoffOpts{
			BaseWait:             1 * time.Second,
			MaxWait:              99999 * time.Hour,
			PenaltyDecayInterval: NoDecay,
			GrowthFactor:         3,
		})
		require.NoError(t, err)
		got, _ := hammerLimiter(t, time.Now(), l, len(want))

		require.InDeltaSlice(t, want, got, 0.01)
	})
	t.Run("Base0.25sFactor2", func(t *testing.T) {
		want := []float64{0.25, 0.5, 1, 2, 4, 8, 16, 32, 64, 128}

		l, err := factory(BackoffOpts{
			BaseWait:             250 * time.Millisecond,
			MaxWait:              99999 * time.Hour,
			PenaltyDecayInterval: NoDecay,
			GrowthFactor:         2,
		})
		require.NoError(t, err)
		got, _ := hammerLimiter(t, time.Now(), l, len(want))

		require.InDeltaSlice(t, want, got, 0.01)
	})
	t.Run("CappedByMaxWait", func(t *testing.T) {
		want := []float64{1, 2, 4, 8, 16, 30, 30, 30, 30}

		l, err := factory(BackoffOpts{
			BaseWait:             1 * time.Second,
			MaxWait:              30 * time.Second,
			PenaltyDecayInterval: NoDecay,
			GrowthFactor:         2,
		})
		require.NoError(t, err)
		got, _ := hammerLimiter(t, time.Now(), l, len(want))

		// Allow 0.01 slop
		require.InDeltaSlice(t, want, got, 0.01)
	})
}

// Ensures that MaxWait is respected, and that PenaltyDecayInterval
// does indeed determine the "memory" of the limiter.
func testBackoffLimiter_MaxWait(
	t *testing.T,
	factory func(opts BackoffOpts) (testableLimiter, error),
) {
	// Scenario outlined in comments for BackoffOpts
	opts := BackoffOpts{
		BaseWait:             time.Second,
		MaxWait:              30 * time.Second,
		PenaltyDecayInterval: 48 * time.Hour,
		GrowthFactor:         2,
	}

	l, err := factory(opts)
	require.NoError(t, err)

	start := time.Now()

	// Rack up a lot of penalty points (caps at 6 based on opts)
	mp := maxPenalty(opts.MaxWait, opts.BaseWait, opts.GrowthFactor)
	n := int(math.Ceil(mp))
	_, now := hammerLimiter(t, start, l, n)

	// Ensure that we weren't asked to wait too long
	took := now.Sub(start)
	require.Less(t, took, opts.MaxWait*20)

	// Advance the clock
	now = now.Add(24 * time.Hour)

	// Ensure that the limiter still has a memory of our past
	// transgressions.
	//
	// First attempt should succeed, since +24h > MaxWait, but
	// the next attempt should fail with MaxWait since the prior
	// penalties we racked up are still remembered.
	var ok bool
	ok, _, err = l.Allow(withInjectedTime(t.Context(), now))
	require.NoError(t, err)
	require.True(t, ok)

	// Should be at MaxWait again.
	var wait time.Duration
	ok, wait, err = l.Allow(withInjectedTime(t.Context(), now))
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, opts.MaxWait, wait)

	// Advance enough time into the future to decay all penalties
	// i.e. ensure the memory is not longer than it should be
	advance := time.Duration(mp * float64(opts.PenaltyDecayInterval))
	now = now.Add(advance)

	ok, _, err = l.Allow(withInjectedTime(t.Context(), now))
	require.NoError(t, err)
	require.True(t, ok)

	// First wait encountered should be BaseWait,
	// all penalties should have expired
	ok, wait, err = l.Allow(withInjectedTime(t.Context(), now))
	require.NoError(t, err)
	require.False(t, ok)
	require.InDelta(t, opts.BaseWait, wait, float64(time.Microsecond))
}

// Ensures that premature subsequent attempts don't grow the penalty,
// which would increase the backoff time.
//
// Guards against timing-related bugs where the next attempt might be
// made slightly too soon, and would lead to a spiral of ever-increasing
// backoffs that could never succeed.
func testBackoffLimiter_PrematureAttemptsDontGrow(
	t *testing.T,
	factory func(opts BackoffOpts) (testableLimiter, error),
) {
	l, err := factory(BackoffOpts{})
	require.NoError(t, err)

	now := time.Now()

	// First attempt should succeed
	ok, _, err := l.Allow(withInjectedTime(t.Context(), now))
	require.NoError(t, err)
	require.True(t, ok)

	// Immediate next attempt should not succeed
	// (remember, clock is frozen)
	ok, wait1, err := l.Allow(withInjectedTime(t.Context(), now))
	require.NoError(t, err)
	require.False(t, ok)
	require.Greater(t, wait1, time.Duration(0))

	// Ensure another attempt doesn't bump the wait time
	ok, wait2, err := l.Allow(withInjectedTime(t.Context(), now))
	require.NoError(t, err)
	require.False(t, ok)
	require.EqualValues(t, wait1, wait2)

	// Third attempt, just in case there was special-case logic for
	// first failure
	ok, wait3, err := l.Allow(withInjectedTime(t.Context(), now))
	require.NoError(t, err)
	require.False(t, ok)
	require.EqualValues(t, wait1, wait3)
}

// Ensures that the wait scales as the penalty decays, and does not remain
// scaled when enough time has passed for the penalty to have decayed to 0
func testBackoffLimiter_WaitScalesWithDecay(
	t *testing.T,
	factory func(opts BackoffOpts) (testableLimiter, error),
) {
	decayTime := 10 * time.Second
	l, err := factory(BackoffOpts{
		BaseWait:             time.Second,
		PenaltyDecayInterval: decayTime,
	})
	require.NoError(t, err)

	now := time.Now()

	// Use the limiter, increasing internal penalty to 1
	ok, _, err := l.Allow(withInjectedTime(t.Context(), now))
	require.NoError(t, err)
	require.True(t, ok)

	// Advance the clock such that the penalty should return back to 0
	now = now.Add(decayTime)

	// Use the limiter, increasing internal penalty back to 1
	ok, _, err = l.Allow(withInjectedTime(t.Context(), now))
	require.NoError(t, err)
	require.True(t, ok)

	// Attempt too soon, while penalty is still == 1
	// Ensure that the waitSecs reflects the BaseWait
	ok, wait, err := l.Allow(withInjectedTime(t.Context(), now))
	require.NoError(t, err)
	require.False(t, ok)
	require.EqualValues(t, time.Second, wait)

	// Attempt
	ok, wait, err = l.Allow(withInjectedTime(t.Context(), now.Add(200*time.Millisecond)))
	require.NoError(t, err)
	require.False(t, ok)

	// Ensure that the wait has decayed to 800ms (1s - 200ms)
	require.EqualValues(t, 800*time.Millisecond, wait)
}

// Ensures that the wait remains scaled if not enough time has passed
// for penalty to have decayed to 0
func testBackoffLimiter_WaitExceedsBaselineBeforeDecay(
	t *testing.T,
	factory func(opts BackoffOpts) (testableLimiter, error),
) {
	decayTime := 10 * time.Second
	l, err := factory(BackoffOpts{
		BaseWait:             time.Second,
		PenaltyDecayInterval: decayTime,
		GrowthFactor:         2,
	})
	require.NoError(t, err)

	now := time.Now()

	// Use the limiter, increasing internal penalty to 1
	ok, _, err := l.Allow(withInjectedTime(t.Context(), now))
	require.NoError(t, err)
	require.True(t, ok)

	// Advance the clock such that the penalty should not return fully
	// back to 1. In this test setup, 1 unit of decay takes 10s, and
	// decay is linear, so decayTime / 2 should leave penalty at 0.5
	now = now.Add(decayTime / 2)

	// Use the limiter, increasing internal penalty by 1
	// Penalty should now be at 1.5 -- i.e. the next wait should be scaled
	ok, _, err = l.Allow(withInjectedTime(t.Context(), now))
	require.NoError(t, err)
	require.True(t, ok)

	// Ensure wait is scaled
	//
	// For penalty of 1.5, wait should be:
	// - BaseWait * (GrowthRate ** (penalty - 1))
	//   = 1s * (2 ** 0.5)
	//   = 1s * 1.41421356237
	//   = 1.41421356237s
	ok, wait, err := l.Allow(withInjectedTime(t.Context(), now))
	require.NoError(t, err)
	require.False(t, ok)

	// Check with a little slop as Redis only keeps microsecond-level precision
	f := 1.41421356237
	require.InDelta(t, time.Duration(f*float64(time.Second)), wait, float64(time.Microsecond))
}

// Ensures that the limiter behaves as expected for a fixed scenario,
// to detect behaviour changes
func testBackoffLimiter_DecayScenario(
	t *testing.T,
	factory func(opts BackoffOpts) (testableLimiter, error),
) {
	l, err := factory(BackoffOpts{
		BaseWait:             500 * time.Millisecond,
		MaxWait:              1795 * time.Second,
		PenaltyDecayInterval: 30 * time.Minute,
		GrowthFactor:         2,
	})
	require.NoError(t, err)

	now := time.Now()

	want1 := []float64{
		0.5, 0.999807477, 1.998845236, 3.994614559,
		7.976949108, 15.90496658, 31.615701873, 62.466251951,
		121.963157083, 232.734984472, 425.568355265, 722.483186458,
		1094.033087329, 1435.805382682, 1651.973511587, 1748.875382074,
		1783.646921739, 1794.91447, 1795, 1795,
	}

	// Make N (20) successful attempts to the limiter, waiting/backing off by the required time
	var gotWaitSecs []float64
	gotWaitSecs, now = hammerLimiter(t, now, l, len(want1))
	require.InDeltaSlice(t, want1, gotWaitSecs, 0.01)

	// Advance clock by 1h40
	advance := 100 * time.Minute
	now = now.Add(advance)

	// Quick check to ensure the advance was big enough to exceed the last
	// backoff time given (otherwise subsequent iterateOverLimiter will fail,
	// as it expects its first call to allow() to succeed).
	require.Greater(t, advance.Seconds(), gotWaitSecs[len(gotWaitSecs)-1])

	// Make N (12) more successful attempts, confirming that a portion of the
	// penalty from earlier is still applied
	want2 := []float64{
		178.429775258, 333.163125137, 586.096452623, 935.364688031,
		1304.913178478, 1578.990023002, 1719.257186382, 1773.553062616,
		1791.707629173, 1795, 1795, 1795,
	}
	gotWaitSecs, now = hammerLimiter(t, now, l, len(want2))
	require.InDeltaSlice(t, want2, gotWaitSecs, 0.01)

	// Advance clock by 24h, enough time for 48x penalty units to have decayed
	// (far in excess of this test)
	now = now.Add(24 * time.Hour)

	// Ensures that no initial limit is applied, and that the backoff
	// sequence has returned to default
	gotWaitSecs, _ = hammerLimiter(t, now, l, len(want1))
	require.InDeltaSlice(t, want1, gotWaitSecs, 0.01)
}

func hammerLimiter(t *testing.T, now time.Time, l testableLimiter, n int) (results []float64, now2 time.Time) {
	// Make N successful attempts to the limiter, waiting the required time
	for i := 0; i < n*2; i++ {
		ok, wait, err := l.Allow(withInjectedTime(t.Context(), now))
		require.NoError(t, err)

		// Every *other* attempt should end up limited, since this logic retries
		// immediately (no time elapsed) after success, then waits the required period.
		lastWasLimited := i%2 == 0

		if ok && !lastWasLimited {
			t.Fatalf("allowed twice in a row")
		}
		if !ok && lastWasLimited {
			t.Fatalf("limited twice in a row")
		}

		if !ok {
			now = now.Add(wait)
			results = append(results, wait.Seconds())
		}
	}

	now2 = now
	return
}
