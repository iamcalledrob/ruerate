package ruebucket

import (
	"context"
	_ "embed"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/redis/rueidis"
)

type BackoffOpts struct {
	// BaseWait is the initial delay used for the first back-off, which
	// then gets increased by GrowthFactor as the backoff escalates.
	//
	// Must be less than MaxWait.
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
	// Penalty increments by 1.0 for each successful call to Allow.
	//
	// PenaltyDecayInterval
	// Because penalty only increments on success, and a success requires
	// waiting for the backoff to expire, the wait time will never exceed
	// (1 / PenaltyDecayRate) seconds.
	//
	// Use with Every, e.g. `Every(time.Minute)`
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

// Sanitize takes a fairly strict view on the supplied options, optimising
// for clarity, rejecting configurations that don't make sense.
func (o *BackoffOpts) Sanitize() error {
	if o == nil {
		return fmt.Errorf("BackoffOpts is nil")
	}
	if o.BaseWait == 0 {
		o.BaseWait = 1 * time.Second
	}
	if o.BaseWait < 0 {
		return fmt.Errorf("BaseWait must not be negative")
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
		return fmt.Errorf("PenaltyDecayRate must not be negative")
	}
	if o.MaxWait == 0 {
		o.MaxWait = o.PenaltyDecayInterval
	}
	if o.MaxWait < 0 {
		return fmt.Errorf("MaxWait must not be negative")
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

type RedisKeyedBackoffOpts struct {
	BackoffOpts BackoffOpts

	// Redis is the Redis key where the limiter state score should be stored
	// for a given limiter key, e.g. limiter:users_by_ip:{key}:penalty
	RedisKey func(key string) string
}

func (o *RedisKeyedBackoffOpts) Sanitize() error {
	if o.RedisKey == nil {
		return fmt.Errorf("RedisKey is required")
	}
	err := o.BackoffOpts.Sanitize()
	if err != nil {
		return fmt.Errorf("BackoffOpts: %w", err)
	}
	return nil
}

type RedisKeyedBackoffLimiter struct {
	client rueidis.Client
	opts   RedisKeyedBackoffOpts
	script *rueidis.Lua
}

func NewRedisKeyedBackoffLimiter(
	client rueidis.Client,
	opts RedisKeyedBackoffOpts,
) (*RedisKeyedBackoffLimiter, error) {
	err := opts.Sanitize()
	if err != nil {
		return nil, fmt.Errorf("opts: %w", err)
	}

	// Opts don't change during limiter lifespan, more efficient to define as constants.
	s := strings.NewReplacer(
		"{{ BASE_WAIT_MICROS }}", strconv.FormatInt(opts.BackoffOpts.BaseWait.Microseconds(), 10),
		"{{ MAX_WAIT_MICROS }}", strconv.FormatInt(opts.BackoffOpts.MaxWait.Microseconds(), 10),
		"{{ PENALTY_DECAY_RATE }}", strconv.FormatFloat(Every(opts.BackoffOpts.PenaltyDecayInterval), 'f', -1, 64),
		"{{ GROWTH_FACTOR }}", strconv.FormatFloat(opts.BackoffOpts.GrowthFactor, 'f', -1, 64),
		"{{ MAX_PENALTY }}", strconv.FormatFloat(opts.BackoffOpts.maxPenalty, 'f', -1, 64),
	).Replace(luaBackoffScript)
	lua := rueidis.NewLuaScript(s)

	return &RedisKeyedBackoffLimiter{
		client: client,
		opts:   opts,
		script: lua,
	}, nil
}

// NewRedisKeyedBackoffLimiterWithDefaultKey is a convenience initializer for a
// RedisKeyedBackoffLimiter that writes to Redis key: "limiter:ID:{KEY}"
func NewRedisKeyedBackoffLimiterWithDefaultKey(
	client rueidis.Client,
	id string,
	opts BackoffOpts,
) (*RedisKeyedBackoffLimiter, error) {
	return NewRedisKeyedBackoffLimiter(client, RedisKeyedBackoffOpts{
		BackoffOpts: opts,
		RedisKey: func(key string) string {
			return "limiter:" + id + ":{" + key + "}"
		},
	})
}

func (l *RedisKeyedBackoffLimiter) Reset(ctx context.Context, key string) error {
	// Deleting keys reverts the limiter to its default state
	cmd := l.client.B().Del().
		Key(l.opts.RedisKey(key)).
		Build()
	return l.client.Do(ctx, cmd).Error()
}

func (l *RedisKeyedBackoffLimiter) Allow(ctx context.Context, key string) (ok bool, wait time.Duration, err error) {
	keys := []string{
		l.opts.RedisKey(key), // lua: state_key
	}
	// Allow for injecting arbitrary time for tests (lua: now_micros)
	// If arg is not sent, redis lua will use redis server time
	var args []string
	if t := injectedTimeFromContext(ctx); t != nil {
		args = append(args, strconv.FormatInt(t.UnixMicro(), 10))
	}

	resp := l.script.Exec(ctx, l.client, keys, args)

	// Redis lua script should respond with array of 2 ints
	var ints []int64
	ints, err = resp.AsIntSlice()
	if err != nil {
		err = fmt.Errorf("acquiring key %s: %w", key, err)
		return
	}
	if len(ints) != 2 {
		err = fmt.Errorf("incorrect result length: %d", len(ints))
		return
	}

	allowed := ints[0] == 1
	if allowed {
		ok = true
		return
	}

	// Convert micros to nanos (time.Duration)
	wait = time.Duration(ints[1] * 1000)

	// Sanity check, lua shouldn't tell you "not allowed, but no wait needed"
	// This has caught a bug caused by Redis's float truncation
	if wait < 1 {
		err = fmt.Errorf("bug: failed to acquire but no wait")
		return
	}

	return
}

type RedisBackoffOpts struct {
	BackoffOpts BackoffOpts

	// RedisKey is the Redis key where the limiter state should be stored,
	// e.g. limiter:{my_global_action}
	RedisKey string
}

func (o *RedisBackoffOpts) Sanitize() error {
	if o.RedisKey == "" {
		return fmt.Errorf("RedisKey is required")
	}

	err := o.BackoffOpts.Sanitize()
	if err != nil {
		return fmt.Errorf("BackoffOpts: %w", err)
	}
	return nil
}

func NewRedisBackoffLimiter(
	client rueidis.Client,
	opts RedisBackoffOpts,
) (*RedisBackoffLimiter, error) {
	err := opts.Sanitize()
	if err != nil {
		return nil, fmt.Errorf("opts: %w", err)
	}

	var l *RedisKeyedBackoffLimiter
	l, err = NewRedisKeyedBackoffLimiter(client, RedisKeyedBackoffOpts{
		BackoffOpts: opts.BackoffOpts,
		RedisKey: func(_ string) string {
			return opts.RedisKey
		},
	})
	if err != nil {
		return nil, fmt.Errorf("instantiating underlying keyed limiter: %w", err)
	}

	return &RedisBackoffLimiter{source: l}, nil
}

// NewBackoffLimiterWithDefaultKeys is a convenience initializer for an
// unkeyed BackoffLimiter that writes to Redis keys:
// - limiter:{ID}:penalty
// - limiter:{ID}:last_acquired_at
func NewBackoffLimiterWithDefaultKeys(
	client rueidis.Client,
	id string,
	opts BackoffOpts,
) (*RedisBackoffLimiter, error) {
	return NewRedisBackoffLimiter(client, RedisBackoffOpts{
		BackoffOpts: opts,
		RedisKey:    "limiter:{" + id + "}",
	})
}

// RedisBackoffLimiter wraps a RedisKeyedBackoffLimiter and limits to a single key
type RedisBackoffLimiter struct {
	source *RedisKeyedBackoffLimiter
}

func (l *RedisBackoffLimiter) Reset(ctx context.Context) error {
	return l.source.Reset(ctx, "")
}

func (l *RedisBackoffLimiter) Allow(ctx context.Context) (ok bool, wait time.Duration, err error) {
	return l.source.Allow(ctx, "")
}

// Allow for injecting a time in a standardised way, for testing.
func withInjectedTime(ctx context.Context, t time.Time) context.Context {
	return context.WithValue(ctx, injectedTimeKey, t)
}

func injectedTimeFromContext(ctx context.Context) *time.Time {
	if v := ctx.Value(injectedTimeKey); v != nil {
		t := v.(time.Time)
		return &t
	}
	return nil
}

type contextKey int

const (
	injectedTimeKey contextKey = 0
)

//go:embed backoff.lua
var luaBackoffScript string
