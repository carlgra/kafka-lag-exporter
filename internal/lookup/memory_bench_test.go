package lookup

import "testing"

func BenchmarkMemoryTable_AddPoint(b *testing.B) {
	table := NewMemoryTable(60)
	// Pre-populate with 30 points.
	for i := range 30 {
		table.AddPoint(Point{Offset: int64(i * 100), Time: int64(1000 + i*1000)})
	}

	b.ResetTimer()
	for i := range b.N {
		// Adding points beyond capacity triggers eviction.
		table.AddPoint(Point{Offset: int64(3000 + i*100), Time: int64(31000 + i*1000)})
	}
}

func BenchmarkMemoryTable_Lookup(b *testing.B) {
	table := NewMemoryTable(60)
	for i := range 60 {
		table.AddPoint(Point{Offset: int64(i * 100), Time: int64(1000 + i*1000)})
	}

	// Lookup at mid-range forces interpolation.
	midOffset := int64(3050)

	b.ResetTimer()
	for range b.N {
		table.Lookup(midOffset)
	}
}

func BenchmarkMemoryTable_AddPoint_Parallel(b *testing.B) {
	table := NewMemoryTable(60)
	for i := range 30 {
		table.AddPoint(Point{Offset: int64(i * 100), Time: int64(1000 + i*1000)})
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// Each goroutine adds different offsets — some will be rejected as
		// non-monotonic/out-of-order, which is fine for measuring lock contention.
		i := 0
		for pb.Next() {
			table.AddPoint(Point{Offset: int64(50000 + i*100), Time: int64(100000 + i*1000)})
			i++
		}
	})
}

func BenchmarkMemoryTable_Lookup_Parallel(b *testing.B) {
	table := NewMemoryTable(60)
	for i := range 60 {
		table.AddPoint(Point{Offset: int64(i * 100), Time: int64(1000 + i*1000)})
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			table.Lookup(3050)
		}
	})
}
