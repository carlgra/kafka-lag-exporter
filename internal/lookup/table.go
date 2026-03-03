// Package lookup provides offset-to-time lookup tables for computing time lag.
package lookup

// Point represents an offset observed at a specific timestamp.
type Point struct {
	Offset int64
	Time   int64 // milliseconds since epoch
}

// AddPointResult indicates the outcome of adding a point to the table.
type AddPointResult int

const (
	Inserted     AddPointResult = iota // New point added successfully
	Updated                            // Existing offset updated (flat line)
	NonMonotonic                       // Offset went backwards — rejected
	OutOfOrder                         // Time went backwards — rejected
	WriteError                         // Backend write failed
)

// LookupResult holds the result of a time lookup for a given offset.
type LookupResult struct {
	// Found is true if a time estimate was produced.
	Found bool
	// Time is the estimated timestamp (ms) when the partition reached the given offset.
	Time int64
	// Reason describes why the lookup failed (empty on success).
	Reason string
}

// LookupTable stores offset-time points and interpolates timestamps for arbitrary offsets.
type LookupTable interface {
	AddPoint(point Point) AddPointResult
	Lookup(offset int64) LookupResult
	Length() int64
}

// predict performs linear interpolation/extrapolation between two points.
func predict(p1, p2 Point, offset int64) int64 {
	if p2.Offset == p1.Offset {
		return p2.Time
	}
	slope := float64(p2.Time-p1.Time) / float64(p2.Offset-p1.Offset)
	return p1.Time + int64(slope*float64(offset-p1.Offset))
}
