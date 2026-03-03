package lookup

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryTable_AddPoint(t *testing.T) {
	table := NewMemoryTable(5)

	// First insert.
	assert.Equal(t, Inserted, table.AddPoint(Point{Offset: 100, Time: 1000}))
	assert.Equal(t, int64(1), table.Length())

	// Normal inserts.
	assert.Equal(t, Inserted, table.AddPoint(Point{Offset: 200, Time: 2000}))
	assert.Equal(t, Inserted, table.AddPoint(Point{Offset: 300, Time: 3000}))
	assert.Equal(t, int64(3), table.Length())

	// Flat line — same offset, different time.
	assert.Equal(t, Updated, table.AddPoint(Point{Offset: 300, Time: 3500}))
	assert.Equal(t, int64(3), table.Length())

	// Non-monotonic — offset went backwards.
	assert.Equal(t, NonMonotonic, table.AddPoint(Point{Offset: 250, Time: 4000}))
	assert.Equal(t, int64(3), table.Length())

	// Out of order — time went backwards.
	assert.Equal(t, OutOfOrder, table.AddPoint(Point{Offset: 400, Time: 2500}))
	assert.Equal(t, int64(3), table.Length())
}

func TestMemoryTable_Eviction(t *testing.T) {
	table := NewMemoryTable(3)

	table.AddPoint(Point{Offset: 100, Time: 1000})
	table.AddPoint(Point{Offset: 200, Time: 2000})
	table.AddPoint(Point{Offset: 300, Time: 3000})
	assert.Equal(t, int64(3), table.Length())

	// Adding a 4th evicts the oldest.
	table.AddPoint(Point{Offset: 400, Time: 4000})
	assert.Equal(t, int64(3), table.Length())

	// Oldest should now be 200; lookup at 100 should extrapolate.
	result := table.Lookup(100)
	require.True(t, result.Found)
	// Extrapolation: oldest(200,2000), latest(400,4000) → slope=10, predict(100) = 2000 + 10*(100-200) = 1000
	assert.Equal(t, int64(1000), result.Time)
}

func TestMemoryTable_Lookup_TooFewPoints(t *testing.T) {
	table := NewMemoryTable(5)

	result := table.Lookup(100)
	assert.False(t, result.Found)
	assert.Equal(t, "TooFewPoints", result.Reason)

	table.AddPoint(Point{Offset: 100, Time: 1000})
	result = table.Lookup(100)
	assert.False(t, result.Found)
}

func TestMemoryTable_Lookup_AtLatest(t *testing.T) {
	table := NewMemoryTable(5)
	table.AddPoint(Point{Offset: 100, Time: 1000})
	table.AddPoint(Point{Offset: 200, Time: 2000})

	// Exactly at latest offset.
	result := table.Lookup(200)
	require.True(t, result.Found)
	assert.Equal(t, int64(2000), result.Time)

	// Beyond latest offset.
	result = table.Lookup(300)
	require.True(t, result.Found)
	assert.Equal(t, int64(2000), result.Time)
}

func TestMemoryTable_Lookup_Interpolation(t *testing.T) {
	table := NewMemoryTable(10)
	table.AddPoint(Point{Offset: 100, Time: 1000})
	table.AddPoint(Point{Offset: 200, Time: 2000})
	table.AddPoint(Point{Offset: 300, Time: 3000})
	table.AddPoint(Point{Offset: 400, Time: 4000})

	// Interpolate between (100,1000) and (200,2000).
	result := table.Lookup(150)
	require.True(t, result.Found)
	assert.Equal(t, int64(1500), result.Time)

	// Interpolate between (200,2000) and (300,3000).
	result = table.Lookup(250)
	require.True(t, result.Found)
	assert.Equal(t, int64(2500), result.Time)
}

func TestMemoryTable_Lookup_Extrapolation(t *testing.T) {
	table := NewMemoryTable(10)
	table.AddPoint(Point{Offset: 200, Time: 2000})
	table.AddPoint(Point{Offset: 400, Time: 4000})

	// Below range — extrapolate.
	result := table.Lookup(100)
	require.True(t, result.Found)
	// Slope = (4000-2000)/(400-200) = 10, predict(100) = 2000 + 10*(100-200) = 1000
	assert.Equal(t, int64(1000), result.Time)
}

func TestPredict(t *testing.T) {
	p1 := Point{Offset: 100, Time: 1000}
	p2 := Point{Offset: 200, Time: 2000}

	assert.Equal(t, int64(1500), predict(p1, p2, 150))
	assert.Equal(t, int64(500), predict(p1, p2, 50))
	assert.Equal(t, int64(2500), predict(p1, p2, 250))

	// Same offset — returns p2.Time.
	p3 := Point{Offset: 100, Time: 3000}
	assert.Equal(t, int64(3000), predict(p1, p3, 150))
}

func TestMemoryTable_MinSize(t *testing.T) {
	// Size < 2 should be clamped to 2.
	table := NewMemoryTable(0)
	assert.Equal(t, int64(0), table.Length())

	table.AddPoint(Point{Offset: 100, Time: 1000})
	table.AddPoint(Point{Offset: 200, Time: 2000})
	assert.Equal(t, int64(2), table.Length())

	// Adding a 3rd should evict the oldest (clamped to size 2).
	table.AddPoint(Point{Offset: 300, Time: 3000})
	assert.Equal(t, int64(2), table.Length())

	// Lookup should work with only 2 points.
	result := table.Lookup(250)
	require.True(t, result.Found)
}

func TestMemoryTable_SizeOne(t *testing.T) {
	table := NewMemoryTable(1) // Clamped to 2.
	table.AddPoint(Point{Offset: 10, Time: 100})
	table.AddPoint(Point{Offset: 20, Time: 200})
	table.AddPoint(Point{Offset: 30, Time: 300})
	assert.Equal(t, int64(2), table.Length())
}
