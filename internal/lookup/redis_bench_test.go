package lookup

import (
	"log/slog"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func setupRedisBench(b *testing.B) (*RedisTable, *miniredis.Miniredis) {
	b.Helper()
	mr := miniredis.RunT(b)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	table := NewRedisTableFromClient(client, "bench:tp", time.Hour, time.Hour, 60, slog.Default())
	return table, mr
}

func BenchmarkRedisTable_AddPoint(b *testing.B) {
	table, _ := setupRedisBench(b)

	// Pre-populate.
	for i := range 30 {
		table.AddPoint(Point{Offset: int64(i * 100), Time: int64(1000 + i*1000)})
	}

	b.ResetTimer()
	for i := range b.N {
		table.AddPoint(Point{Offset: int64(3000 + i*100), Time: int64(31000 + i*1000)})
	}
}

func BenchmarkRedisTable_Lookup(b *testing.B) {
	table, _ := setupRedisBench(b)

	for i := range 60 {
		table.AddPoint(Point{Offset: int64(i * 100), Time: int64(1000 + i*1000)})
	}

	midOffset := int64(3050)

	b.ResetTimer()
	for range b.N {
		table.Lookup(midOffset)
	}
}
