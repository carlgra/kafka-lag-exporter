package lookup

import "sync"

// MemoryTable is an in-memory LookupTable backed by a fixed-size sliding window.
type MemoryTable struct {
	mu     sync.RWMutex
	points []Point
	size   int
}

// NewMemoryTable creates a MemoryTable with the given maximum window size.
func NewMemoryTable(size int) *MemoryTable {
	if size < 2 {
		size = 2
	}
	return &MemoryTable{
		points: make([]Point, 0, size),
		size:   size,
	}
}

func (t *MemoryTable) AddPoint(point Point) AddPointResult {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.points) == 0 {
		t.points = append(t.points, point)
		return Inserted
	}

	last := t.points[len(t.points)-1]

	// Time went backwards.
	if point.Time < last.Time {
		return OutOfOrder
	}

	// Offset went backwards.
	if point.Offset < last.Offset {
		return NonMonotonic
	}

	// Same offset — flat line, update the timestamp of the last point.
	if point.Offset == last.Offset {
		t.points[len(t.points)-1] = point
		return Updated
	}

	// New valid point.
	t.points = append(t.points, point)

	// Evict oldest if over capacity.
	if len(t.points) > t.size {
		t.points = t.points[len(t.points)-t.size:]
	}

	return Inserted
}

func (t *MemoryTable) Lookup(offset int64) LookupResult {
	t.mu.RLock()
	defer t.mu.RUnlock()

	n := len(t.points)
	if n < 2 {
		return LookupResult{Reason: "TooFewPoints"}
	}

	oldest := t.points[0]
	latest := t.points[n-1]

	// Offset matches or exceeds latest — lag is zero.
	if offset >= latest.Offset {
		return LookupResult{Found: true, Time: latest.Time}
	}

	// Within range — find surrounding points and interpolate.
	for i := 0; i < n-1; i++ {
		p1, p2 := t.points[i], t.points[i+1]
		if offset >= p1.Offset && offset < p2.Offset {
			return LookupResult{
				Found: true,
				Time:  predict(p1, p2, offset),
			}
		}
	}

	// Below range — extrapolate from oldest to latest.
	return LookupResult{
		Found: true,
		Time:  predict(oldest, latest, offset),
	}
}

func (t *MemoryTable) Length() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return int64(len(t.points))
}
