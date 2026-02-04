# ruerate
ruelimit implements high-performance rate limiting algorithms with both in-memory Golang and distributed Redis implementations.

Distributed variants built on top of [redis/rueidis](https://github.com/redis/rueidis) with auto-pipelining.
The in-memory variants use a sharded lockable map.

[Godoc](https://pkg.go.dev/github.com/iamcalledrob/ruerate)


## Features
1. **Zero memory default**: Only limiters out of their default state use memory. Automatic cleanup via TTLs.
2. **Keyed limiters**: Apply limits per-identifier (e.g. http requests by IP)
3. **Replenishable**: Manually add tokens back to the bucket on-demand (token bucket algorithm)
4. **Local caching**: `RedisCacheable*` variants avoid round-trips when a limiter is known to be exhausted
5. **Very low allocation**: Most paths are zero-alloc, others are 1 alloc.

# Usage

## Token Bucket Algorithm
Use-cases: API rate limiting, protecting from traffic spikes, and scenarios requiring a steady flow of requests
with occasional bursts.

### Implementations:
- `tokenbucket.LocalLimiter`: Single-resource in-memory limiter.
- `tokenbucket.LocalKeyedLimiter`: Per-key in-memory limiter.
- `tokenbucket.RedisLimiter`: Distributed, replenishable single-resource limiter.
- `tokenbucket.RedisKeyedLimiter`: Distributed, replenishable per-key limiter.
- `tokenbucket.RedisCacheableLimiter`: More efficient version of `RedisLimiter` with client-side caching.
- `tokenbucket.RedisCacheableKeyedLimiter`: More efficient version of `RedisKeyedLimiter` with client-side caching.

### Configuration
The `tokenbucket.LimiterOpts` struct defines the bucket behavior:
```
type LimiterOpts struct {
    RatePerSec float64 // How many tokens are added to the bucket per second
    Capacity   int64   // Maximum tokens the bucket can hold
}
```

### Example
```golang
// Scenario: Allow 1 HTTP request per IP every second, with a burst of up to 100.
//
// Use distributed, Redis-backed variant so limits apply across frontend server pool.
lim, _ := tokenbucket.RedisCacheableKeyedLimiter(
    rueidisClient,
    tokenbucket.DefaultRedisKeyedLimiterOpts(
        "http_requests_by_ip", // limiter name, used to derive Redis key
        LimiterOpts{
            RatePerSec: ruerate.Every(time.Second), 
            Capacity: 100,
        }
    )
)

// Check if this IP is allowed to make another HTTP request
ok, wait, _ := lim.Allow(context.TODO(), "10.0.0.1")

if !ok {
    // Not allowed -- no tokens left in the bucket
    // Next token available after 'wait'
    w.WriteHeader(http.StatusTooManyRequests)
    // You probably want to apply jitter. This will add rand(0-50%) jitter.
    wait = ruerate.WithJitter(wait, 0.5)
    w.Header().Set("X-RateLimit-Remaining-Ms", strconv.FormatInt(wait.Milliseconds(), 10))
}

// Handle the request...
```


## Exponential Backoff, Linear Decay Algorithm
Use-cases: Jailing spammy clients, slowing down brute-force login attempts, or building polite client-side reconnection
loops, i.e. with the local in-memory variants.

### Implementations:
- `backoff.LocalLimiter`: Single resource in-memory backoff limiter
- `backoff.LocalKeyedLimiter`: Per-key in-memory backoff limiter
- `backoff.RedisLimiter`: Distributed, single resource backoff limiter
- `backoff.RedisKeyedLimiter`: Distributed, per-key backoff limiter

### Configuration
The `backoff.LimiterOpts` struct defines the bucket behavior:
```
type LimiterOpts struct {
    BaseWait              time.Duration // Minimum wait between attempts
    MaxWait               time.Duration // Maximum "lockout" duration
    PenaltyDecayInterval  time.Duration // How long prior attempts take to be "forgotten"
    GrowthFactor          float64       // How fast backoff grows. 2.0 doubles each time.
}
```


### Example (server)
```golang
// Scenario: Game server should punish overly frequent reconnections, based on UserID.
// Users shouldn't be reconnecting more often than once per 30 mins (in the long run)
// Be tolerant to occasional reconnects, but quickly ramp "punishment" for too many.
lim, _ := backoff.NewLocalKeyedLimiter(
    backoff.LimiterOpts{
        // Minimum wait of 5s between attempts. Wait grows from here.
        BaseWait: 5 * time.Second,
		
        // Users can be "locked out" for a maximum of 300s
        MaxWait: 300 * time.Second,
		
        // Prior attempts take 30m to be forgotten (linear decay)
        PenaltyDecayInterval: 30 * time.Minute,
        
        // Wait doubles each time, e.g. 5s, 10s, 20s, 40s, 1m20s, 2m40s etc. (up to MaxWait cap)
        GrowthFactor: 2.0,
    }
)

// Check if this user is allowed to connect right now
ok, wait, _ := lim.Allow(context.TODO(), "timapple")
if !ok {
    // Logic to reject connection, probably applying jitter to wait...
}

```

### Example (client)
```golang
// Scenario: Apply back-off when connecting to a server.
// Start at 1s between attempts, and double each time until 32s between attempts.
// Remember failed attempts for 60s, will "cool down" back to 1s after about 5 minutes.
lim, _ := backoff.NewLocalLimiter(
    backoff.LimiterOpts{
        BaseWait: time.Second,
        MaxWait: 32 * time.Second,
        PenaltyDecayInterval: 60 * time.Second,
        GrowthFactor: 2.0,
    }
})

// Connection loop sketch
// Will wait 1s, 2s, 4s, 8s, 16s, 32s, 32s, 32s (-decay)
func DialServerWithBackoff(ctx context.Context, lim *LocalBackoffLimiter) error {
retry:
    ok, wait, _ := lim.Allow(ctx)
    if !ok {
        select {
        case <-time.After(wait):
            goto retry
        case <-ctx.Done():
            return fmt.Errorf("while waiting: %w", err)
        }	
    }
	
    err := server.Dial(ctx)
	if errors.Is(err, ErrServerUnreachable) {
        goto retry
    }
	
    return err
}
```

## Performance
Redis variants are designed to use as little Redis keyspace and storage as possible, and reduce round-trips where
possible, via `RedisCacheable*` variants. These limiters are designed for use in hot paths in production.

By using rueidis auto-pipelining, exceptional throughput is possible, and the simple atomic design makes it simple
to scale via hash-slot sharding. Due to pipelining support, benchmarks should be run with high parallelism in order to benchmark the limiter, not RTT.
Benchmarks run against Valkey 8.1.x running locally.

### tokenbucket algo
```
goos: linux
goarch: amd64
pkg: github.com/iamcalledrob/ruerate/tokenbucket
cpu: AMD Ryzen 5 8600G w/ Radeon 760M Graphics     

BenchmarkRedisCacheableLimiter
BenchmarkRedisCacheableLimiter/Exhausted_Serial
BenchmarkRedisCacheableLimiter/Exhausted_Serial-12         	 6212396	       190.3 ns/op	       0 B/op	       0 allocs/op
BenchmarkRedisCacheableLimiter/Exhausted_Parallel100
BenchmarkRedisCacheableLimiter/Exhausted_Parallel100-12    	 5814763	       208.5 ns/op	       0 B/op	       0 allocs/op
BenchmarkRedisCacheableLimiter/Allowed_Parallel100
BenchmarkRedisCacheableLimiter/Allowed_Parallel100-12      	  238906	      4725 ns/op	      99 B/op	       1 allocs/op

BenchmarkRedisLimiter
BenchmarkRedisLimiter/Exhausted_Parallel100
BenchmarkRedisLimiter/Exhausted_Parallel100-12    	  347024	      3381 ns/op	      93 B/op	       1 allocs/op
BenchmarkRedisLimiter/Allowed_Parallel100
BenchmarkRedisLimiter/Allowed_Parallel100-12      	  247245	      4706 ns/op	      98 B/op	       1 allocs/op

BenchmarkLocalLimiter
BenchmarkLocalLimiter/Exhausted_Serial
BenchmarkLocalLimiter/Exhausted_Serial-12         	12621777	        98.36 ns/op	       0 B/op	       0 allocs/op
BenchmarkLocalLimiter/Exhausted_Parallel100
BenchmarkLocalLimiter/Exhausted_Parallel100-12    	 9617128	       128.4 ns/op	       0 B/op	       0 allocs/op
BenchmarkLocalLimiter/Allowed_Parallel100
BenchmarkLocalLimiter/Allowed_Parallel100-12      	11479965	       125.3 ns/op	       0 B/op	       0 allocs/op

```

### backoff algo
```
goos: linux
goarch: amd64
pkg: github.com/iamcalledrob/ruerate/tokenbucket
cpu: AMD Ryzen 5 8600G w/ Radeon 760M Graphics     

BenchmarkRedisLimiter
BenchmarkRedisLimiter/Allow_Serial
BenchmarkRedisLimiter/Allow_Serial-12         	  185803	      6146 ns/op	     102 B/op	       1 allocs/op
BenchmarkRedisLimiter/Allow_Parallel100
BenchmarkRedisLimiter/Allow_Parallel100-12    	  356449	      3164 ns/op	      92 B/op	       1 allocs/op

BenchmarkLocalLimiter
BenchmarkLocalLimiter/Allow_Serial
BenchmarkLocalLimiter/Allow_Serial-12         	15445687	        78.62 ns/op	       0 B/op	       0 allocs/op
BenchmarkLocalLimiter/Allow_Parallel100
BenchmarkLocalLimiter/Allow_Parallel100-12    	11196720	       120.3 ns/op	       0 B/op	       0 allocs/op

BenchmarkLocalKeyedLimiter
BenchmarkLocalKeyedLimiter/Allow_Serial
BenchmarkLocalKeyedLimiter/Allow_Serial-12         	 6896593	       171.1 ns/op	       0 B/op	       0 allocs/op
BenchmarkLocalKeyedLimiter/Allow_Parallel100
BenchmarkLocalKeyedLimiter/Allow_Parallel100-12    	 6183844	       196.1 ns/op	       0 B/op	       0 allocs/op

```

## TODOs
- [x] Migrate to common interface across local and redis token bucket limiters
- [x] MILD backoff algorithm
- [ ] Leaky bucket algorithm
- [ ] backoff.RedisCacheableLimiter, to avoid any Redis round-trip when a client is known to been locked out.
