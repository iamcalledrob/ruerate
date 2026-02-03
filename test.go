package ruerate

import (
	"math"
	"os"
	"testing"
	"time"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

func NewTestRedisClient(t testing.TB) rueidis.Client {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{addr},
		SelectDB:    1, // don't clobber default db (0)
	})
	require.NoError(t, err)

	// Erase the entire db to isolate tests from each other.
	err = client.Do(t.Context(), client.B().Flushdb().Build()).Error()
	require.NoError(t, err)

	return client
}

// Every converts a time interval to a rate per second for use with *Limiter constructors.
// Similar to rate.Every, but avoids taking a dependency on that package.
func Every(interval time.Duration) float64 {
	if interval <= 0 {
		return math.MaxFloat64
	}
	return 1 / interval.Seconds()
}

var AllowEvery = Every // Backwards compat
