-- Token bucket that supports both adding (replenishing) and removing tokens
-- Stores no state in redis when the bucket is completely full

local tokens_key = KEYS[1]
local last_acquired_at_key = KEYS[2]

local rate_per_sec = tonumber(ARGV[1])
local bucket_capacity = tonumber(ARGV[2])
local tokens_delta = tonumber(ARGV[3])

local redis_time = redis.call("TIME")
local now_micros = tonumber(redis_time[1]) * 1e6 + tonumber(redis_time[2])

-- Get how many tokens are in the bucket
-- If no key exists, the bucket is full.
local last_tokens   = tonumber(redis.call("GET", tokens_key) or bucket_capacity)
local last_acquired_at_micros = tonumber(redis.call("GET", last_acquired_at_key) or now_micros)

-- Convert rate to per-microsecond
local rate_per_micro = rate_per_sec / 1e6

-- Calculate how many tokens have accrued as a result of the passage of time
local delta_micros = math.max(0, now_micros - last_acquired_at_micros)
local accrued_tokens = delta_micros * rate_per_micro

-- Calculate how many tokens the bucket would have after applying accrued tokens
-- and the requested delta. Constrain to bucket capacity.
local current_tokens = math.min(last_tokens + accrued_tokens + tokens_delta, bucket_capacity)

-- Acquisition would take tokens negative: not allowed
if current_tokens < 0 then
    -- Calculate wait until acquisition could be possible
    local shortfall = 0 - current_tokens
    local wait_micros = math.ceil(shortfall / rate_per_micro)
    -- Result: {Not Allowed, Wait wait_micros}
    return {0, wait_micros}
end

-- Allowed: Calculate space left in the bucket
local tokens_until_full = bucket_capacity - current_tokens

if tokens_until_full <= 0 then
    -- Bucket has been completely filled
    redis.call("DEL", tokens_key, last_acquired_at_key)
else
    -- Auto-expire key at time it would become full.
    -- A missing key is treated as a full bucket.
    local time_until_full_millis = math.ceil(tokens_until_full / (rate_per_sec / 1000))
    redis.call("SET", tokens_key, current_tokens, "PX", time_until_full_millis)
    redis.call("SET", last_acquired_at_key, now_micros, "PX", time_until_full_millis)
end
-- Result: {Allowed, No Wait}
return {1, 0}

