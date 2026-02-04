package backoff

import (
	"context"
	_ "embed"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/iamcalledrob/ruebucket"
	"github.com/redis/rueidis"
)

type RedisKeyedLimiterOpts struct {
	LimiterOpts LimiterOpts

	// Redis is the Redis key where the limiter state score should be stored
	// for a given limiter key, e.g. limiter:users_by_ip:{key}
	RedisKey func(key string) string
}

func (o *RedisKeyedLimiterOpts) Sanitize() error {
	if o.RedisKey == nil {
		return fmt.Errorf("RedisKey is required")
	}
	err := o.LimiterOpts.Sanitize()
	if err != nil {
		return fmt.Errorf("BackoffOpts: %w", err)
	}
	return nil
}

type RedisKeyedLimiter struct {
	client rueidis.Client
	opts   RedisKeyedLimiterOpts
	script *rueidis.Lua
}

// NewRedisKeyedBackoffLimiter instantiates a new redis-backed keyed backoff limiter.
// Errors returned will always relate to sanity of the provided opts.
// It would be reasonable to panic on error, e.g. lo.Must()
func NewRedisKeyedBackoffLimiter(
	client rueidis.Client,
	opts RedisKeyedLimiterOpts,
) (*RedisKeyedLimiter, error) {
	err := opts.Sanitize()
	if err != nil {
		return nil, fmt.Errorf("opts: %w", err)
	}

	// Opts don't change during limiter lifespan, more efficient to define as constants.
	s := strings.NewReplacer(
		"{{ BASE_WAIT_MICROS }}", strconv.FormatInt(opts.LimiterOpts.BaseWait.Microseconds(), 10),
		"{{ MAX_WAIT_MICROS }}", strconv.FormatInt(opts.LimiterOpts.MaxWait.Microseconds(), 10),
		"{{ PENALTY_DECAY_INTERVAL_MICROS }}", strconv.FormatInt(opts.LimiterOpts.PenaltyDecayInterval.Microseconds(), 10),
		"{{ PENALTY_DECAY_RATE }}", strconv.FormatFloat(ruerate.Every(opts.LimiterOpts.PenaltyDecayInterval), 'f', -1, 64),
		"{{ GROWTH_FACTOR }}", strconv.FormatFloat(opts.LimiterOpts.GrowthFactor, 'f', -1, 64),
		"{{ MAX_PENALTY }}", strconv.FormatFloat(opts.LimiterOpts.maxPenalty, 'f', -1, 64),
	).Replace(luaBackoffScript)
	lua := rueidis.NewLuaScript(s)

	return &RedisKeyedLimiter{
		client: client,
		opts:   opts,
		script: lua,
	}, nil
}

// NewRedisKeyedBackoffLimiterWithDefaultKey is a convenience initializer for a
// RedisKeyedLimiter that writes to Redis key: "limiter:ID:{KEY}"
func NewRedisKeyedBackoffLimiterWithDefaultKey(
	client rueidis.Client,
	id string,
	opts LimiterOpts,
) (*RedisKeyedLimiter, error) {
	return NewRedisKeyedBackoffLimiter(client, RedisKeyedLimiterOpts{
		LimiterOpts: opts,
		RedisKey: func(key string) string {
			return "limiter:" + id + ":{" + key + "}"
		},
	})
}

func (l *RedisKeyedLimiter) Reset(ctx context.Context, key string) error {
	// Deleting keys reverts the limiter to its default state
	cmd := l.client.B().Del().
		Key(l.opts.RedisKey(key)).
		Build()
	return l.client.Do(ctx, cmd).Error()
}

func (l *RedisKeyedLimiter) Allow(ctx context.Context, key string) (ok bool, wait time.Duration, err error) {
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

type RedisLimiterOpts struct {
	LimiterOpts LimiterOpts

	// RedisKey is the Redis key where the limiter state should be stored,
	// e.g. limiter:{my_global_action}
	RedisKey string
}

func (o *RedisLimiterOpts) Sanitize() error {
	if o.RedisKey == "" {
		return fmt.Errorf("RedisKey is required")
	}
	err := o.LimiterOpts.Sanitize()
	if err != nil {
		return fmt.Errorf("BackoffOpts: %w", err)
	}
	return nil
}

func NewRedisBackoffLimiter(
	client rueidis.Client,
	opts RedisLimiterOpts,
) (*RedisLimiter, error) {
	err := opts.Sanitize()
	if err != nil {
		return nil, fmt.Errorf("opts: %w", err)
	}

	var l *RedisKeyedLimiter
	l, err = NewRedisKeyedBackoffLimiter(client, RedisKeyedLimiterOpts{
		LimiterOpts: opts.LimiterOpts,
		RedisKey: func(_ string) string {
			return opts.RedisKey
		},
	})
	if err != nil {
		return nil, fmt.Errorf("instantiating underlying keyed limiter: %w", err)
	}

	return &RedisLimiter{source: l}, nil
}

// NewRedisLimiterWithDefaultKeys is a convenience initializer for an
// unkeyed BackoffLimiter that writes to Redis key "limiter:{ID}"
func NewRedisLimiterWithDefaultKeys(
	client rueidis.Client,
	id string,
	opts LimiterOpts,
) (*RedisLimiter, error) {
	return NewRedisBackoffLimiter(client, RedisLimiterOpts{
		LimiterOpts: opts,
		RedisKey:    "limiter:{" + id + "}",
	})
}

// RedisLimiter wraps a RedisKeyedLimiter and limits to a single key
type RedisLimiter struct {
	source *RedisKeyedLimiter
}

func (l *RedisLimiter) Reset(ctx context.Context) error {
	return l.source.Reset(ctx, "")
}

func (l *RedisLimiter) Allow(ctx context.Context) (ok bool, wait time.Duration, err error) {
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
