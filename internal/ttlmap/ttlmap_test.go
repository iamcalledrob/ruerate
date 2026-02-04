package ttlmap

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Ensures that items can be retrieved immediately after being set.
func TestSetGet(t *testing.T) {
	m := New[string, int]()
	shard := m.Shard("foo")
	shard.Lock()
	defer shard.Unlock()

	expiry := time.Now().Add(time.Minute)
	shard.Set("foo", 42, expiry)
	val, expiresAt, ok := shard.Get("foo")
	require.True(t, ok)
	require.Equal(t, 42, val)
	require.Equal(t, expiry, expiresAt)
}

// Ensures that items are no longer available once their TTL has passed.
func TestExpiry(t *testing.T) {
	m := New[string, int]()
	shard := m.Shard("foo")
	shard.Lock()
	defer shard.Unlock()

	now := time.Now()
	expiry := now.Add(time.Millisecond * 10)
	shard.Set("foo", 42, expiry)
	time.Sleep(time.Millisecond * 20)

	_, _, ok := shard.Get("foo")
	require.False(t, ok)
}

// Ensures that the amortised cleanup (collectGarbage) eventually clears expired items.
// This demonstrates the design decision to clean 3 random items per Get call.
func TestAmortisedCleanup(t *testing.T) {
	m := New[string, int]()
	shard := m.Shard("foo")
	shard.Lock()
	defer shard.Unlock()

	now := time.Now()
	expiry := now.Add(10 * time.Millisecond)

	for i := 0; i < 100; i++ {
		shard.Set(fmt.Sprintf("key_%d", i), i, expiry)
	}

	// Expire all items
	time.Sleep(20 * time.Millisecond)

	// Call Get on a non-existent key several times to trigger collectGarbage
	for i := 0; i < 40; i++ {
		shard.Get("nonexistent")
	}

	// GC should have cleaned up this many keys at least.
	require.Less(t, len(shard.entries), 50)
}

// Benchmark the overhead of hashing and shard selection.
func BenchmarkShardSelection(b *testing.B) {
	m := New[int, int]()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Shard(i)
	}
}

// Benchmark Set (includes locking and map insertion).
func BenchmarkSet(b *testing.B) {
	m := New[int, int]()
	expiry := time.Now().Add(time.Minute)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := m.Shard(i)
		s.Lock()
		s.Set(i, i, expiry)
		s.Unlock()
	}
}

// Benchmark Get ops with amortised GC overhead.
//
// goos: linux
// goarch: amd64
// pkg: github.com/iamcalledrob/ruebucket/internal/ttlmap
// cpu: AMD Ryzen 5 8600G w/ Radeon 760M Graphics
// BenchmarkGetWithGC
// BenchmarkGetWithGC/size=100
// BenchmarkGetWithGC/size=100-12         	13131565	        85.43 ns/op
// BenchmarkGetWithGC/size=1000
// BenchmarkGetWithGC/size=1000-12        	12825656	        95.32 ns/op
// BenchmarkGetWithGC/size=10000
// BenchmarkGetWithGC/size=10000-12       	12860983	        93.58 ns/op
// BenchmarkGetWithGC/size=100000
// BenchmarkGetWithGC/size=100000-12      	11695909	       100.5 ns/op
// BenchmarkGetWithGC/size=1000000
// BenchmarkGetWithGC/size=1000000-12     	 4499842	       247.2 ns/op
func BenchmarkGetWithGC(b *testing.B) {
	sizes := []int{100, 1_000, 10_000, 100_000, 1_000_000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			m := New[int, int]()
			s := m.Shard(1)

			// Populate the map with 10K items for a more realistic scenario
			s.Lock()
			for i := 0; i < size; i++ {
				s.Set(i, 1, time.Now().Add(time.Minute))
			}
			s.Unlock()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.Lock()
				s.Get(1)
				s.Unlock()
			}
		})
	}
}
