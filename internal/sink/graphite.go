package sink

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"math"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/seglo/kafka-lag-exporter/internal/metrics"
)

// GraphiteSink sends metrics to a Graphite server via TCP (optionally TLS) protocol.
// It maintains a persistent connection and buffers metrics for batched writes.
type GraphiteSink struct {
	host       string
	port       int
	prefix     string
	maxBufSize int
	tlsEnabled bool
	filter     *MetricFilter
	logger     *slog.Logger

	mu   sync.Mutex
	conn net.Conn
	buf  bytes.Buffer
}

// Default maximum buffer size for Graphite (10MB).
const defaultGraphiteMaxBufSize = 10 * 1024 * 1024

// NewGraphiteSink creates a new Graphite sink.
func NewGraphiteSink(host string, port int, prefix string, useTLS bool, filter *MetricFilter, logger *slog.Logger) *GraphiteSink {
	return &GraphiteSink{
		host:       host,
		port:       port,
		prefix:     prefix,
		maxBufSize: defaultGraphiteMaxBufSize,
		tlsEnabled: useTLS,
		filter:     filter,
		logger:     logger,
	}
}

func (s *GraphiteSink) Report(_ context.Context, m metrics.MetricValue) {
	if s.filter != nil && !s.filter.Matches(m.Definition.Name) {
		return
	}
	if math.IsNaN(m.Value) || math.IsInf(m.Value, 0) {
		return
	}

	path := s.buildPath(m)
	now := time.Now().Unix()
	line := fmt.Sprintf("%s %v %d\n", path, m.Value, now)

	s.mu.Lock()
	if s.maxBufSize > 0 && s.buf.Len()+len(line) > s.maxBufSize {
		s.logger.Warn("graphite buffer full, dropping metric", "metric", m.Definition.Name, "bufSize", s.buf.Len())
		s.mu.Unlock()
		return
	}
	s.buf.WriteString(line)
	s.mu.Unlock()
}

// Flush sends all buffered metrics over the persistent TCP connection.
// It lazily connects on first call and reconnects on failure.
// Data is only cleared from the buffer on a successful write.
func (s *GraphiteSink) Flush() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.buf.Len() == 0 {
		return
	}

	// Copy buffer so data is preserved on write failure.
	data := make([]byte, s.buf.Len())
	copy(data, s.buf.Bytes())

	if err := s.ensureConnected(); err != nil {
		s.logger.Warn("graphite connection failed, data preserved in buffer", "error", err)
		return
	}

	_ = s.conn.SetWriteDeadline(time.Now().Add(DefaultWriteTimeout))
	if _, err := s.conn.Write(data); err != nil {
		s.logger.Warn("graphite write failed, reconnecting", "error", err)
		_ = s.conn.Close()
		s.conn = nil

		// Retry once with a fresh connection.
		if err := s.ensureConnected(); err != nil {
			s.logger.Warn("graphite reconnect failed, data preserved in buffer", "error", err)
			return
		}
		_ = s.conn.SetWriteDeadline(time.Now().Add(DefaultWriteTimeout))
		if _, err := s.conn.Write(data); err != nil {
			s.logger.Warn("graphite retry write failed, data preserved in buffer", "error", err)
			_ = s.conn.Close()
			s.conn = nil
			return
		}
	}

	// Only reset buffer after successful write.
	s.buf.Reset()
}

func (s *GraphiteSink) ensureConnected() error {
	if s.conn != nil {
		return nil
	}
	addr := net.JoinHostPort(s.host, fmt.Sprintf("%d", s.port))

	if s.tlsEnabled {
		dialer := &tls.Dialer{
			NetDialer: &net.Dialer{Timeout: DefaultDialTimeout},
			Config:    &tls.Config{MinVersion: tls.VersionTLS12},
		}
		conn, err := dialer.DialContext(context.Background(), "tcp", addr)
		if err != nil {
			return fmt.Errorf("TLS dialing %s: %w", addr, err)
		}
		s.conn = conn
		return nil
	}

	conn, err := net.DialTimeout("tcp", addr, DefaultDialTimeout)
	if err != nil {
		return fmt.Errorf("dialing %s: %w", addr, err)
	}
	s.conn = conn
	return nil
}

func (s *GraphiteSink) Remove(_ context.Context, _ metrics.RemoveMetric) {
	// Graphite does not support metric removal.
}

func (s *GraphiteSink) Stop() {
	s.Flush()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
	}
}

// buildPath constructs the dot-separated Graphite metric path.
func (s *GraphiteSink) buildPath(m metrics.MetricValue) string {
	var parts []string

	if s.prefix != "" {
		parts = append(parts, sanitize(s.prefix))
	}

	for _, name := range m.Definition.Labels {
		parts = append(parts, sanitize(m.Labels[name]))
	}

	parts = append(parts, sanitize(m.Definition.Name))
	return strings.Join(parts, ".")
}

// sanitize replaces dots with underscores in Graphite path components.
func sanitize(s string) string {
	return strings.ReplaceAll(s, ".", "_")
}
