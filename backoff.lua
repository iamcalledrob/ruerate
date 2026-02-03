-- Exponential increase, linear decrease limiter to match
-- behaviour of Golang limiter defined in backoff_local.go

local state_key = KEYS[1]

local base_wait_micros = {{ BASE_WAIT_MICROS }}
local max_wait_micros = {{ MAX_WAIT_MICROS }}
-- per_sec (not per_micro) because values can be very small even at this scale:
--   e.g. decay every 30 mins = per_sec rate of 0.000555556
-- Don't want to run into floating point precision issues
local penalty_decay_rate_per_sec = {{ PENALTY_DECAY_RATE }}
local growth_factor = {{ GROWTH_FACTOR }}
local max_penalty = {{ MAX_PENALTY }}

local redis_time = redis.call("TIME")
local now_micros = tonumber(redis_time[1]) * 1e6 + tonumber(redis_time[2])

-- Time can be injected for testing, specify a non-zero value
if ARGV[1] and tonumber(ARGV[1]) ~= 0 then
    now_micros = tonumber(ARGV[1])
end

-- Get current penalty and last acquisition time from redis hash, if exists.
-- If no key exists, the limiter is in its default state
local state = redis.call("HMGET", state_key, "p", "la")
local penalty = tonumber(state[1] or 0)
local last_acquired_at_micros = tonumber(state[2] or 0)

-- Calculate microseconds since the last successful acquisition.
-- Safety: clamp to 0 to prevent "penalty growth" if the system clock drifts backwards,
-- which would cause elapsed to go negative.
local elapsed_micros = math.max(0, now_micros - last_acquired_at_micros)

-- Apply linear decay to the penalty based on how much time has passed
-- Don't allow penalty to go negative
if last_acquired_at_micros > 0 then
    local penalty_decay = (elapsed_micros / 1e6) * penalty_decay_rate_per_sec
    penalty = math.max(0, penalty - penalty_decay)
end

-- Given the current penalty, how long until the next event is allowed
local backoff_micros = 0
if penalty > 0 then
    -- Note: X ** 0 = 1, so the penalty > 0 check ensures that a zero penalty
    -- results in no wait, rather than erroneously defaulting to the BaseWait.
    --
    -- We use max(0, l.penalty-1) as the exponent to ensure the first penalty,
    -- e.g. 1, is not scaled by GrowthFactor.
    local exp = math.max(0, penalty - 1)
    backoff_micros = math.min(
        base_wait_micros * math.pow(growth_factor, exp),
        max_wait_micros
    )
end

-- Return remaining wait time if the backoff period hasn't fully elapsed.
-- math.ceil is applied to ensure waits in the 0..<1 microsec range do not get
-- truncated down to 0 by default number return behaviour from lua scripts.
-- This would yield an unexpected result where the caller is not allowed, but
-- the remaining wait time is 0.
local wait_micros = math.ceil(backoff_micros - elapsed_micros)
if wait_micros > 0 then
    -- Result: {Not Allowed, Wait wait_micros}
    return {0, wait_micros}
end

-- Successful acquisition: increment penalty and update state.
-- Clamp penalty to maxPenalty (derived from MaxWait) so that the
-- "internal debt" never exceeds the "external wait" ceiling.
penalty = math.min(penalty + 1, max_penalty)

-- ttl_ms, aka time until penalty would decay to zero
local ttl_ms = math.ceil((penalty / penalty_decay_rate_per_sec) * 1000)

-- Persist state in a single key
redis.call("HSET", state_key, "p", penalty, "la", now_micros)
redis.call("PEXPIRE", state_key, ttl_ms)

-- Result: {Allowed, No Wait}
return {1, 0}