package lookup

import (
	"log/slog"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestRedis(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return client, mr
}

func TestRedisTable_AddAndLookup(t *testing.T) {
	client, _ := newTestRedis(t)
	defer func() { _ = client.Close() }()

	key := "test:redis_table:add_lookup"
	table := NewRedisTableFromClient(client, key, time.Hour, time.Hour, 100, slog.Default())

	// Too few points.
	result := table.Lookup(100)
	assert.False(t, result.Found)
	assert.Equal(t, "TooFewPoints", result.Reason)

	// Add points.
	assert.Equal(t, Inserted, table.AddPoint(Point{Offset: 100, Time: 1000}))
	assert.Equal(t, Inserted, table.AddPoint(Point{Offset: 200, Time: 2000}))
	assert.Equal(t, Inserted, table.AddPoint(Point{Offset: 300, Time: 3000}))
	assert.Equal(t, int64(3), table.Length())

	// Interpolate.
	result = table.Lookup(150)
	require.True(t, result.Found)
	assert.Equal(t, int64(1500), result.Time)

	// At latest.
	result = table.Lookup(300)
	require.True(t, result.Found)
	assert.Equal(t, int64(3000), result.Time)

	// Beyond latest.
	result = table.Lookup(500)
	require.True(t, result.Found)
	assert.Equal(t, int64(3000), result.Time)

	// Below range — extrapolate.
	result = table.Lookup(50)
	require.True(t, result.Found)
	// slope = (3000-1000)/(300-100) = 10, predict(50) = 1000 + 10*(50-100) = 500
	assert.Equal(t, int64(500), result.Time)
}

func TestRedisTable_FlatLine(t *testing.T) {
	client, _ := newTestRedis(t)
	defer func() { _ = client.Close() }()

	key := "test:redis_table:flat_line"
	table := NewRedisTableFromClient(client, key, time.Hour, time.Hour, 100, slog.Default())

	assert.Equal(t, Inserted, table.AddPoint(Point{Offset: 100, Time: 1000}))
	assert.Equal(t, Updated, table.AddPoint(Point{Offset: 100, Time: 2000}))
	assert.Equal(t, int64(1), table.Length())
}

func TestRedisTable_Ordering(t *testing.T) {
	client, _ := newTestRedis(t)
	defer func() { _ = client.Close() }()

	key := "test:redis_table:ordering"
	table := NewRedisTableFromClient(client, key, time.Hour, time.Hour, 100, slog.Default())

	assert.Equal(t, Inserted, table.AddPoint(Point{Offset: 200, Time: 2000}))
	assert.Equal(t, NonMonotonic, table.AddPoint(Point{Offset: 100, Time: 3000}))
	assert.Equal(t, OutOfOrder, table.AddPoint(Point{Offset: 300, Time: 1000}))
}

func TestRedisTable_MaxSize(t *testing.T) {
	client, _ := newTestRedis(t)
	defer func() { _ = client.Close() }()

	key := "test:redis_table:maxsize"
	table := NewRedisTableFromClient(client, key, time.Hour, time.Hour, 3, slog.Default())

	table.AddPoint(Point{Offset: 100, Time: 1000})
	table.AddPoint(Point{Offset: 200, Time: 2000})
	table.AddPoint(Point{Offset: 300, Time: 3000})
	assert.Equal(t, int64(3), table.Length())

	table.AddPoint(Point{Offset: 400, Time: 4000})
	// Should trim to maxSize=3, evicting oldest.
	assert.Equal(t, int64(3), table.Length())
}

func TestRedisTable_Close(t *testing.T) {
	client, _ := newTestRedis(t)
	key := "test:redis_table:close"
	table := NewRedisTableFromClient(client, key, time.Hour, time.Hour, 100, slog.Default())

	err := table.Close()
	assert.NoError(t, err)
}

func TestNewRedisTable(t *testing.T) {
	mr := miniredis.RunT(t)

	cfg := RedisTableConfig{
		Host:       mr.Host(),
		Port:       mr.Server().Addr().Port,
		Database:   0,
		Password:   "",
		Timeout:    5 * time.Second,
		Prefix:     "kafka-lag-exporter",
		Separator:  ":",
		Retention:  time.Hour,
		Expiration: time.Hour,
		MaxSize:    100,
	}

	table := NewRedisTable(cfg, "test-cluster", "test-topic", 0, slog.Default())
	require.NotNil(t, table)
	assert.Equal(t, "kafka-lag-exporter:test-cluster:test-topic:0", table.key)

	// Should be able to add and lookup.
	assert.Equal(t, Inserted, table.AddPoint(Point{Offset: 10, Time: 100}))
	assert.Equal(t, Inserted, table.AddPoint(Point{Offset: 20, Time: 200}))
	assert.Equal(t, int64(2), table.Length())

	result := table.Lookup(15)
	require.True(t, result.Found)
	assert.Equal(t, int64(150), result.Time)
}

func TestRedisTable_LookupOnePoint(t *testing.T) {
	client, _ := newTestRedis(t)
	defer func() { _ = client.Close() }()

	key := "test:redis_table:one_point"
	table := NewRedisTableFromClient(client, key, time.Hour, time.Hour, 100, slog.Default())

	table.AddPoint(Point{Offset: 100, Time: 1000})
	result := table.Lookup(100)
	assert.False(t, result.Found) // Only 1 point, need at least 2.
	assert.Equal(t, "TooFewPoints", result.Reason)
}

func TestRedisTable_NoMaxSize(t *testing.T) {
	client, _ := newTestRedis(t)
	defer func() { _ = client.Close() }()

	key := "test:redis_table:no_maxsize"
	table := NewRedisTableFromClient(client, key, time.Hour, time.Hour, 0, slog.Default())

	for i := int64(0); i < 10; i++ {
		table.AddPoint(Point{Offset: i * 100, Time: i * 1000})
	}
	// With maxSize=0, no trimming should happen.
	assert.Equal(t, int64(10), table.Length())
}
