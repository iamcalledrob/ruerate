package ruerate

import (
	"fmt"
	"math"
	"time"
)

type BackoffOpts struct {
	// BaseWait is the initial delay used for the first back-off, which
	// then gets increased by GrowthFactor as the backoff escalates.
	//
	// Must be less than or equal to MaxWait.
	//
	// Defaults to 1s
	BaseWait time.Duration

	// MaxWait caps the maximum duration a caller will ever be required to
	// wait.
	//
	// It allows for a long "memory" (via PenaltyDecayInterval) without completely
	// bricking a user's access, e.g. by sending them a 1-week wait time.
	//
	// Example: you could have a PenaltyDecayInterval of 48 * time.Hour,
	// and a MaxWait of 30 * time.Seconds. This would mean that an offender
	// that causes a burst of activity would quickly be limited to only
	// performing an action every 30s, and if they came back the next day,
	// the limit would still be in place.
	//
	// MaxWait cannot be greater than PenaltyDecayInterval, because this
	// would mean MaxWait could never be reached, as the penalty would decay
	// first.
	//
	// Defaults to PenaltyDecayInterval
	MaxWait time.Duration

	// PenaltyDecayInterval is the time taken for a single penalty point to decay.
	// This determines how long the "memory" of the system is -- i.e. how long
	// it takes for an offender to return to zero-penalty after requests stop.
	//
	// The penalty score increments by 1.0 for each successful call to Allow.
	//
	// Because the penalty only increments on success, and a success requires
	// waiting for the backoff to expire, the wait time between attempts
	// can never exceed PenaltyDecayInterval
	//
	// Defaults to 60s
	PenaltyDecayInterval time.Duration

	// GrowthFactor controls how aggressively wait times ramp up.
	// A factor of 2.0 doubles for each subsequent successful Allow.
	// Defaults to 2
	GrowthFactor float64

	// The maximum value that a penalty can reach before equilibrium
	// is reached with decay.
	// Calculated once, derived from MaxWait, GrowthFactor, BaseWait
	maxPenalty float64
}

// Sanitize checks BackoffOpts fields and sets defaults.
// It is strict about correctness of field values, optimising for clarity,
// rejecting nonsensical configurations.
func (o *BackoffOpts) Sanitize() error {
	if o == nil {
		return fmt.Errorf("BackoffOpts is nil")
	}
	if o.BaseWait == 0 {
		o.BaseWait = 1 * time.Second
	}
	if o.BaseWait < 0 {
		return fmt.Errorf("BaseWait (%s) must not be negative", o.BaseWait)
	}
	if o.PenaltyDecayInterval == 0 {
		o.PenaltyDecayInterval = 60 * time.Second
	}
	// Would make it impossible to accrue penalties beyond 1
	if o.PenaltyDecayInterval < o.BaseWait {
		return fmt.Errorf("PenaltyDecayInterval (%s) < BaseWait (%s)", o.PenaltyDecayInterval, o.BaseWait)
	}
	// Would cause penalty to *increase* with time
	if o.PenaltyDecayInterval < 0 {
		return fmt.Errorf("PenaltyDecayRate (%s) must not be negative", o.PenaltyDecayInterval)
	}
	if o.MaxWait == 0 {
		o.MaxWait = o.PenaltyDecayInterval
	}
	if o.MaxWait < 0 {
		return fmt.Errorf("MaxWait (%s) must not be negative", o.MaxWait)
	}
	if o.MaxWait < o.BaseWait {
		return fmt.Errorf("MaxWait (%s) < BaseWait (%s)", o.MaxWait, o.BaseWait)
	}
	if o.GrowthFactor == 0 {
		o.GrowthFactor = 2
	}
	// < 1 would cause backoff to shrink at each stage
	// = 1 would never grow backoff, and causes divide-by-zero when calculating maxPenalty
	if o.GrowthFactor <= 1 {
		return fmt.Errorf("GrowthFactor (%.2f) must not be <= 1", o.GrowthFactor)
	}
	o.maxPenalty = maxPenalty(o.MaxWait, o.BaseWait, o.GrowthFactor)
	return nil
}

func maxPenalty(maxWait time.Duration, baseWait time.Duration, growthFactor float64) float64 {
	ratio := maxWait.Seconds() / baseWait.Seconds()
	return math.Log(ratio)/math.Log(growthFactor) + 1
}
