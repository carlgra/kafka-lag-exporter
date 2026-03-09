package kafka

import (
	"net"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ClientMetrics tracks Kafka client connection statistics via franz-go hooks.
type ClientMetrics struct {
	connects    atomic.Int64
	disconnects atomic.Int64
	writeErrors atomic.Int64
	readErrors  atomic.Int64
}

// Ensure ClientMetrics implements the hook interfaces at compile time.
var (
	_ kgo.HookBrokerConnect    = (*ClientMetrics)(nil)
	_ kgo.HookBrokerDisconnect = (*ClientMetrics)(nil)
	_ kgo.HookBrokerWrite      = (*ClientMetrics)(nil)
	_ kgo.HookBrokerRead       = (*ClientMetrics)(nil)
)

// OnBrokerConnect tracks successful broker connections.
func (m *ClientMetrics) OnBrokerConnect(_ kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	if err == nil {
		m.connects.Add(1)
	}
}

// OnBrokerDisconnect tracks broker disconnections.
func (m *ClientMetrics) OnBrokerDisconnect(_ kgo.BrokerMetadata, _ net.Conn) {
	m.disconnects.Add(1)
}

// OnBrokerWrite tracks write errors to brokers.
func (m *ClientMetrics) OnBrokerWrite(_ kgo.BrokerMetadata, _ int16, _ int, _, _ time.Duration, err error) {
	if err != nil {
		m.writeErrors.Add(1)
	}
}

// OnBrokerRead tracks read errors from brokers.
func (m *ClientMetrics) OnBrokerRead(_ kgo.BrokerMetadata, _ int16, _ int, _, _ time.Duration, err error) {
	if err != nil {
		m.readErrors.Add(1)
	}
}

// Connects returns the total number of successful broker connections.
func (m *ClientMetrics) Connects() int64 { return m.connects.Load() }

// Disconnects returns the total number of broker disconnections.
func (m *ClientMetrics) Disconnects() int64 { return m.disconnects.Load() }

// WriteErrors returns the total number of write errors.
func (m *ClientMetrics) WriteErrors() int64 { return m.writeErrors.Load() }

// ReadErrors returns the total number of read errors.
func (m *ClientMetrics) ReadErrors() int64 { return m.readErrors.Load() }
