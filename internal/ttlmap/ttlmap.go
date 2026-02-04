package ttlmap

import (
	"hash/maphash"
	"sync"
	"time"
)

// Map is a sharded map with sharded locks with automatic, amortised cleanup
// based on item expiry times.
// Should be pretty efficient and low drama
type Map[K comparable, V any] = sharder[K, Shard[K, V]]

type Opts struct {
	// Number of internal map shards. Must be power of 2. Defaults to 32.
	Shards int
}

func NewWithOpts[K comparable, V any](opts Opts) *Map[K, V] {
	if opts.Shards == 0 {
		opts.Shards = 32
	}
	return newSharder[K, Shard[K, V]](opts.Shards)
}

func New[K comparable, V any]() *Map[K, V] {
	return NewWithOpts[K, V](Opts{})
}

type Shard[K comparable, V any] struct {
	mu      sync.Mutex
	entries map[K]entry[V]
	now     *time.Time
}

func (m *Shard[K, V]) Lock() {
	m.mu.Lock()
}

func (m *Shard[K, V]) Unlock() {
	m.mu.Unlock()
}

// Get must be called while Lock is held
func (m *Shard[K, V]) Get(key K) (v V, expiresAt time.Time, ok bool) {
	var now time.Time
	if m.now != nil {
		now = *m.now
	} else {
		now = time.Now()
	}

	if m.entries == nil {
		m.entries = make(map[K]entry[V])
	} else {
		// Perform incremental garbage collection of expired items
		m.collectGarbage(now)
	}

	var e entry[V]
	e, ok = m.entries[key]
	if !ok {
		return
	}

	if now.After(e.expiresAt) {
		delete(m.entries, key)
		ok = false
		return
	}

	return e.value, e.expiresAt, ok
}

func (m *Shard[K, V]) GetValue(key K) (v V, ok bool) {
	v, _, ok = m.Get(key)
	return
}

func (m *Shard[K, V]) collectGarbage(now time.Time) {
	checks := 3

	// Check {checks} items for expiry in the map in random order
	for k, v := range m.entries {
		if now.After(v.expiresAt) {
			delete(m.entries, k)
		}
		checks--
		if checks == 0 {
			break
		}
	}
}

// Set must be called while Lock is held
func (m *Shard[K, V]) Set(key K, value V, expiresAt time.Time) {
	if m.entries == nil {
		m.entries = make(map[K]entry[V])
	}

	m.entries[key] = entry[V]{value: value, expiresAt: expiresAt}
}

// Delete must be called while Lock is held
func (m *Shard[K, V]) Delete(key K) {
	delete(m.entries, key)
}

// SetNow allows defining "now" time on a shard.
// Only intended for use when testing.
// When nil, uses time.Now()
func (m *Shard[K, V]) SetNow(now *time.Time) {
	m.now = now
}

type entry[V any] struct {
	value     V
	expiresAt time.Time
}

type sharder[K comparable, S any] struct {
	shards []S
	seed   maphash.Seed
	mask   uint64
}

func newSharder[K comparable, S any](size int) *sharder[K, S] {
	mask := uint64(size - 1)
	if uint64(size)&(mask) != 0 {
		panic("sharder: size must be power of 2")
	}
	return &sharder[K, S]{
		shards: make([]S, size),
		seed:   maphash.MakeSeed(),
		mask:   mask,
	}
}

func (s *sharder[K, S]) Shard(key K) *S {
	// Perform fast bitwise lookup based on hash and mask
	return &s.shards[maphash.Comparable(s.seed, key)&s.mask]
}
