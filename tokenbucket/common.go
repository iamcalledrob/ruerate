package tokenbucket

import (
	"context"
	"fmt"
	"time"
)

type LimiterOpts struct {
	// RatePerSec is the replenish rate of the bucket.
	// i.e. how many tokens get deposited in the bucket per second.
	// Required.
	RatePerSec float64

	// Capacity is the bucket's maximum capacity.
	// Buckets start full.
	// Required.
	Capacity int
}

func (o *LimiterOpts) Sanitize() error {
	if o.RatePerSec < 0 {
		return fmt.Errorf("RatePerSec (%.2f) cannot be negative", o.RatePerSec)
	}
	if o.RatePerSec == 0 {
		return fmt.Errorf("RatePerSec is required")
	}
	if o.Capacity == 0 {
		return fmt.Errorf("Capacity is required")
	}
	return nil
}

type CacheableLimiter interface {
	Allow(ctx context.Context) (ok bool, wait time.Duration, err error)
	// The non-replenishable optimisation doesn't support AllowN, because not enough about the server state is known.
}

type Limiter interface {
	CacheableLimiter

	AllowN(ctx context.Context, n int) (ok bool, wait time.Duration, err error)

	Replenish(ctx context.Context) error
	ReplenishN(ctx context.Context, n int) error
}

type CacheableKeyedLimiter interface {
	Allow(ctx context.Context, key string) (ok bool, wait time.Duration, err error)
	// The non-replenishable optimisation doesn't support AllowN, because not enough about the server state is known.
}

type KeyedLimiter interface {
	CacheableKeyedLimiter

	AllowN(ctx context.Context, key string, n int) (ok bool, wait time.Duration, err error)

	Replenish(ctx context.Context, key string) error
	ReplenishN(ctx context.Context, key string, n int) error
}
