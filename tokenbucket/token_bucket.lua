-- Token bucket that supports both adding (replenishing) and removing tokens
-- Stores no state in redis when the bucket is completely full

local state_key = KEYS[1]

local rate_per_sec = {{ RATE_PER_SEC }}
local bucket_capacity = {{ BUCKET_CAPACITY }}
local tokens_delta = tonumber(ARGV[1])

local redis_time = redis.call("TIME")
local now_micros = tonumber(redis_time[1]) * 1e6 + tonumber(redis_time[2])

-- Get how many tokens are in the bucket
-- If no key exists, the bucket is full.
local state = redis.call("HMGET", state_key, "t", "la")
local last_tokens = tonumber(state[1] or bucket_capacity)
local last_acquired_at_micros = tonumber(state[2] or now_micros)

-- Constrain integers to max supported value, prevent runtime error
local int_max = 9007199254740991

-- Convert rate to per-microsecond
local rate_per_micro = rate_per_sec / 1e6

-- Calculate how many tokens have accrued as a result of the passage of time
local delta_micros = math.max(0, now_micros - last_acquired_at_micros)
local accrued_tokens = delta_micros * rate_per_micro

-- Calculate how many tokens the bucket would have after applying accrued tokens
-- and the requested delta. Constrain to bucket capacity.
local current_tokens = math.min(
        last_tokens + accrued_tokens + tokens_delta,
        bucket_capacity
)

-- Acquisition would take tokens negative: not allowed
if current_tokens < 0 then
    -- Calculate wait until acquisition could be possible
    local shortfall = 0 - current_tokens
    local wait_micros = math.ceil(shortfall / rate_per_micro)

    -- Result: {Not Allowed, Wait wait_micros}
    -- Don't allow int to overflow when received by Redis
    return {0, math.min(wait_micros, int_max)}
end

-- Allowed: Calculate space left in the bucket
local tokens_until_full = bucket_capacity - current_tokens

if tokens_until_full <= 0 then
    -- Bucket has been completely filled
    redis.call("DEL", state_key)
else
    -- Auto-expire key at time it would become full.
    -- A missing key is treated as a full bucket.
    -- Don't allow int to overflow when received by Redis
    local time_until_full_millis = math.min(
        math.ceil(tokens_until_full / (rate_per_sec / 1000)),
        int_max
    )

    -- Persist state in a single key
    -- Merge into single HSETEX call once support is more common
    redis.call("HSET", state_key, "t", current_tokens, "la", now_micros)
    redis.call("PEXPIRE", state_key, time_until_full_millis)
end
-- Result: {Allowed, No Wait}
return {1, 0}

