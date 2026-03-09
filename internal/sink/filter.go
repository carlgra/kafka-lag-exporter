package sink

import "regexp"

// MetricFilter determines if a metric should be reported based on allowlist patterns.
type MetricFilter struct {
	patterns []*regexp.Regexp
}

// NewMetricFilter compiles the given regex patterns into a metric filter.
// Go's regexp package uses the RE2 engine which guarantees linear-time matching,
// so user-supplied patterns cannot cause catastrophic backtracking (ReDoS).
func NewMetricFilter(patterns []string) (*MetricFilter, error) {
	compiled := make([]*regexp.Regexp, 0, len(patterns))
	for _, p := range patterns {
		re, err := regexp.Compile(p)
		if err != nil {
			return nil, err
		}
		compiled = append(compiled, re)
	}
	return &MetricFilter{patterns: compiled}, nil
}

// Matches returns true if the metric name matches any allowlist pattern.
func (f *MetricFilter) Matches(metricName string) bool {
	for _, re := range f.patterns {
		if re.MatchString(metricName) {
			return true
		}
	}
	return false
}
