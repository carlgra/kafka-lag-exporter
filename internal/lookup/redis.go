package lookup

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisTable is a LookupTable backed by Redis sorted sets.
// Each topic-partition maps to a sorted set where score=offset and member=timestamp.
type RedisTable struct {
	client     *redis.Client
	key        string
	retention  time.Duration
	expiration time.Duration
	maxSize    int64
	logger     *slog.Logger
}

// RedisTableConfig configures a RedisTable.
type RedisTableConfig struct {
	Host       string
	Port       int
	Database   int
	Password   string
	Timeout    time.Duration
	Prefix     string
	Separator  string
	Retention  time.Duration
	Expiration time.Duration
	MaxSize    int64
	TLS        bool
}

// NewRedisClient creates a shared Redis client from the given config.
// Callers should create one client and pass it to NewRedisTable for each
// topic-partition, rather than creating a client per table.
func NewRedisClient(cfg RedisTableConfig) *redis.Client {
	opts := &redis.Options{
		Addr:        fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		DB:          cfg.Database,
		Password:    cfg.Password,
		DialTimeout: cfg.Timeout,
	}
	if cfg.TLS {
		opts.TLSConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}
	return redis.NewClient(opts)
}

// NewRedisTable creates a new RedisTable for a specific topic-partition
// using a shared Redis client.
func NewRedisTable(client *redis.Client, cfg RedisTableConfig, clusterName, topic string, partition int32, logger *slog.Logger) *RedisTable {
	key := fmt.Sprintf("%s%s%s%s%s%s%d",
		cfg.Prefix, cfg.Separator,
		clusterName, cfg.Separator,
		topic, cfg.Separator,
		partition,
	)

	return &RedisTable{
		client:     client,
		key:        key,
		retention:  cfg.Retention,
		expiration: cfg.Expiration,
		maxSize:    cfg.MaxSize,
		logger:     logger.With("key", key),
	}
}

// NewRedisTableFromClient creates a RedisTable with an existing Redis client (for testing).
func NewRedisTableFromClient(client *redis.Client, key string, retention, expiration time.Duration, maxSize int64, logger *slog.Logger) *RedisTable {
	return &RedisTable{
		client:     client,
		key:        key,
		retention:  retention,
		expiration: expiration,
		maxSize:    maxSize,
		logger:     logger,
	}
}

func (t *RedisTable) AddPoint(point Point) AddPointResult {
	ctx := context.Background()

	// Check the latest point to validate ordering.
	latest, err := t.client.ZRevRangeWithScores(ctx, t.key, 0, 0).Result()
	if err != nil && err != redis.Nil {
		t.logger.Warn("redis read failed", "error", err)
		return WriteError
	}

	if len(latest) > 0 {
		lastOffset := int64(latest[0].Score)
		memberStr, ok := latest[0].Member.(string)
		if !ok {
			t.logger.Warn("redis member type assertion failed", "member", latest[0].Member)
			return WriteError
		}
		lastTime, _ := strconv.ParseInt(memberStr, 10, 64)

		if point.Time < lastTime {
			return OutOfOrder
		}
		if point.Offset < lastOffset {
			return NonMonotonic
		}
		if point.Offset == lastOffset {
			// Update existing — remove old, add new with same score.
			pipe := t.client.Pipeline()
			pipe.ZRemRangeByScore(ctx, t.key, fmt.Sprintf("%d", lastOffset), fmt.Sprintf("%d", lastOffset))
			pipe.ZAdd(ctx, t.key, redis.Z{
				Score:  float64(point.Offset),
				Member: strconv.FormatInt(point.Time, 10),
			})
			pipe.Expire(ctx, t.key, t.expiration)
			if _, err := pipe.Exec(ctx); err != nil {
				t.logger.Warn("redis write failed", "error", err)
				return WriteError
			}
			return Updated
		}
	}

	// Insert new point.
	pipe := t.client.Pipeline()
	pipe.ZAdd(ctx, t.key, redis.Z{
		Score:  float64(point.Offset),
		Member: strconv.FormatInt(point.Time, 10),
	})

	// Trim to max size if configured.
	if t.maxSize > 0 {
		pipe.ZRemRangeByRank(ctx, t.key, 0, -(t.maxSize + 1))
	}

	pipe.Expire(ctx, t.key, t.expiration)

	if _, err := pipe.Exec(ctx); err != nil {
		t.logger.Warn("redis write failed", "error", err)
		return WriteError
	}

	return Inserted
}

func (t *RedisTable) Lookup(offset int64) LookupResult {
	ctx := context.Background()

	count, err := t.client.ZCard(ctx, t.key).Result()
	if err != nil || count < 2 {
		return LookupResult{Reason: "TooFewPoints"}
	}

	offsetStr := fmt.Sprintf("%d", offset)

	// Fetch the point at or below the target offset (the one just before).
	below, err := t.client.ZRevRangeByScoreWithScores(ctx, t.key, &redis.ZRangeBy{
		Min:   "-inf",
		Max:   offsetStr,
		Count: 1,
	}).Result()
	if err != nil {
		return LookupResult{Reason: "RedisError"}
	}

	// Fetch the point above the target offset.
	above, err := t.client.ZRangeByScoreWithScores(ctx, t.key, &redis.ZRangeBy{
		Min:   "(" + offsetStr,
		Max:   "+inf",
		Count: 1,
	}).Result()
	if err != nil {
		return LookupResult{Reason: "RedisError"}
	}

	// If we have a point at or below and offset matches or exceeds it with no point above,
	// the consumer is at or beyond latest.
	if len(below) > 0 && len(above) == 0 {
		memberStr, ok := below[0].Member.(string)
		if !ok {
			return LookupResult{Reason: "TypeAssertionFailed"}
		}
		ts, _ := strconv.ParseInt(memberStr, 10, 64)
		return LookupResult{Found: true, Time: ts}
	}

	// If we have points on both sides, interpolate between them.
	if len(below) > 0 && len(above) > 0 {
		belowStr, ok1 := below[0].Member.(string)
		aboveStr, ok2 := above[0].Member.(string)
		if !ok1 || !ok2 {
			return LookupResult{Reason: "TypeAssertionFailed"}
		}
		belowTs, _ := strconv.ParseInt(belowStr, 10, 64)
		aboveTs, _ := strconv.ParseInt(aboveStr, 10, 64)

		p1 := Point{Offset: int64(below[0].Score), Time: belowTs}
		p2 := Point{Offset: int64(above[0].Score), Time: aboveTs}
		return LookupResult{
			Found: true,
			Time:  predict(p1, p2, offset),
		}
	}

	// Offset is below all stored points — extrapolate from oldest to latest.
	oldest, err := t.client.ZRangeWithScores(ctx, t.key, 0, 0).Result()
	if err != nil || len(oldest) == 0 {
		return LookupResult{Reason: "TooFewPoints"}
	}
	latest, err := t.client.ZRevRangeWithScores(ctx, t.key, 0, 0).Result()
	if err != nil || len(latest) == 0 {
		return LookupResult{Reason: "TooFewPoints"}
	}

	oldestStr, ok1 := oldest[0].Member.(string)
	latestStr, ok2 := latest[0].Member.(string)
	if !ok1 || !ok2 {
		return LookupResult{Reason: "TypeAssertionFailed"}
	}
	oldestTs, _ := strconv.ParseInt(oldestStr, 10, 64)
	latestTs, _ := strconv.ParseInt(latestStr, 10, 64)

	p1 := Point{Offset: int64(oldest[0].Score), Time: oldestTs}
	p2 := Point{Offset: int64(latest[0].Score), Time: latestTs}
	return LookupResult{
		Found: true,
		Time:  predict(p1, p2, offset),
	}
}

func (t *RedisTable) Length() int64 {
	ctx := context.Background()
	count, err := t.client.ZCard(ctx, t.key).Result()
	if err != nil {
		return 0
	}
	return count
}

// Close is a no-op — the shared Redis client is managed externally.
func (t *RedisTable) Close() error {
	return nil
}
