package metrics

// MetricValue represents a metric to be reported to a sink.
type MetricValue struct {
	Definition GaugeDefinition
	Labels     map[string]string // label name → label value
	Value      float64
}

// RemoveMetric requests removal of a metric from a sink.
type RemoveMetric struct {
	Definition GaugeDefinition
	Labels     map[string]string
}
