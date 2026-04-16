package kafka

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/seglo/kafka-lag-exporter/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestMergeProperties_BothNil(t *testing.T) {
	result := mergeProperties(nil, nil)
	assert.NotNil(t, result)
	assert.Empty(t, result)
}

func TestMergeProperties_SingleSide(t *testing.T) {
	a := map[string]string{"key1": "val1"}
	result := mergeProperties(a, nil)
	assert.Equal(t, "val1", result["key1"])

	result = mergeProperties(nil, a)
	assert.Equal(t, "val1", result["key1"])
}

func TestMergeProperties_KeyCollision(t *testing.T) {
	a := map[string]string{"key": "from-a", "only-a": "a"}
	b := map[string]string{"key": "from-b", "only-b": "b"}
	result := mergeProperties(a, b)

	assert.Equal(t, "from-b", result["key"], "b should override a")
	assert.Equal(t, "a", result["only-a"])
	assert.Equal(t, "b", result["only-b"])
}

func TestMergeProperties_DoesNotMutateOriginals(t *testing.T) {
	a := map[string]string{"k": "va"}
	b := map[string]string{"k": "vb"}
	_ = mergeProperties(a, b)
	assert.Equal(t, "va", a["k"])
}

func TestBuildSASLOpt_PLAIN(t *testing.T) {
	props := map[string]string{
		"sasl.username": "user",
		"sasl.password": "pass",
	}
	opt, err := buildSASLOpt("PLAIN", props)
	require.NoError(t, err)
	assert.NotNil(t, opt)
}

func TestBuildSASLOpt_SCRAM_SHA_256(t *testing.T) {
	props := map[string]string{
		"sasl.jaas.config.username": "user",
		"sasl.jaas.config.password": "pass",
	}
	opt, err := buildSASLOpt("SCRAM-SHA-256", props)
	require.NoError(t, err)
	assert.NotNil(t, opt)
}

func TestBuildSASLOpt_SCRAM_SHA_512(t *testing.T) {
	props := map[string]string{
		"sasl.username": "user",
		"sasl.password": "pass",
	}
	opt, err := buildSASLOpt("SCRAM-SHA-512", props)
	require.NoError(t, err)
	assert.NotNil(t, opt)
}

func TestBuildSASLOpt_CaseInsensitive(t *testing.T) {
	props := map[string]string{
		"sasl.username": "user",
		"sasl.password": "pass",
	}
	opt, err := buildSASLOpt("plain", props)
	require.NoError(t, err)
	assert.NotNil(t, opt)
}

func TestBuildSASLOpt_Unsupported(t *testing.T) {
	_, err := buildSASLOpt("OAUTHBEARER", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported SASL mechanism")
}

func TestBuildSASLOpt_JaasConfigTakesPrecedence(t *testing.T) {
	props := map[string]string{
		"sasl.username":             "fallback-user",
		"sasl.jaas.config.username": "primary-user",
		"sasl.password":             "fallback-pass",
		"sasl.jaas.config.password": "primary-pass",
	}
	// If jaas config keys are present, they should be used.
	// We can only verify no error since the returned Opt is opaque.
	opt, err := buildSASLOpt("PLAIN", props)
	require.NoError(t, err)
	assert.NotNil(t, opt)
}

func TestBuildTLSConfig_EmptyProps(t *testing.T) {
	logger := noopLogger()
	cfg, err := buildTLSConfig(map[string]string{}, logger)
	require.NoError(t, err)
	assert.Nil(t, cfg.RootCAs)
	assert.Empty(t, cfg.Certificates)
}

func TestBuildTLSConfig_NonexistentTruststore(t *testing.T) {
	logger := noopLogger()
	props := map[string]string{
		"ssl.truststore.location": "/nonexistent/ca.pem",
	}
	_, err := buildTLSConfig(props, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reading CA certificate")
}

func TestBuildTLSConfig_NonexistentKeystore(t *testing.T) {
	logger := noopLogger()
	props := map[string]string{
		"ssl.keystore.location": "/nonexistent/cert.pem",
		"ssl.key.location":      "/nonexistent/key.pem",
	}
	_, err := buildTLSConfig(props, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "loading client certificate")
}

func TestBuildTLSConfig_InvalidCACert(t *testing.T) {
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(caFile, []byte("not-a-cert"), 0600))

	logger := noopLogger()
	props := map[string]string{
		"ssl.truststore.location": caFile,
	}
	_, err := buildTLSConfig(props, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse CA certificate")
}

func noopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// --- ClientMetrics tests ----------------------------------------------------

func TestClientMetrics_OnBrokerConnect_Success(t *testing.T) {
	m := &ClientMetrics{}
	m.OnBrokerConnect(kgo.BrokerMetadata{}, 0, nil, nil)
	assert.Equal(t, int64(1), m.Connects())
}

func TestClientMetrics_OnBrokerConnect_Error(t *testing.T) {
	m := &ClientMetrics{}
	m.OnBrokerConnect(kgo.BrokerMetadata{}, 0, nil, assert.AnError)
	assert.Equal(t, int64(0), m.Connects())
}

func TestClientMetrics_OnBrokerDisconnect(t *testing.T) {
	m := &ClientMetrics{}
	m.OnBrokerDisconnect(kgo.BrokerMetadata{}, nil)
	m.OnBrokerDisconnect(kgo.BrokerMetadata{}, nil)
	assert.Equal(t, int64(2), m.Disconnects())
}

func TestClientMetrics_OnBrokerWrite_Success(t *testing.T) {
	m := &ClientMetrics{}
	m.OnBrokerWrite(kgo.BrokerMetadata{}, 0, 100, 0, 0, nil)
	assert.Equal(t, int64(0), m.WriteErrors())
}

func TestClientMetrics_OnBrokerWrite_Error(t *testing.T) {
	m := &ClientMetrics{}
	m.OnBrokerWrite(kgo.BrokerMetadata{}, 0, 0, 0, 0, assert.AnError)
	assert.Equal(t, int64(1), m.WriteErrors())
}

func TestClientMetrics_OnBrokerRead_Success(t *testing.T) {
	m := &ClientMetrics{}
	m.OnBrokerRead(kgo.BrokerMetadata{}, 0, 100, 0, 0, nil)
	assert.Equal(t, int64(0), m.ReadErrors())
}

func TestClientMetrics_OnBrokerRead_Error(t *testing.T) {
	m := &ClientMetrics{}
	m.OnBrokerRead(kgo.BrokerMetadata{}, 0, 0, 0, 0, assert.AnError)
	assert.Equal(t, int64(1), m.ReadErrors())
}

// --- TLS constructor tests --------------------------------------------------
//
// These are regression tests for a bug where NewFranzClient always appended
// kgo.Dialer(...) to the options slice and additionally appended
// kgo.DialTLSConfig(...) when security.protocol was SSL. franz-go rejects that
// combination at NewClient time with "cannot set both Dialer and
// DialTLSConfig", so any SSL-enabled cluster failed at startup.
//
// We cover three flavours of SSL config so the table of property-name variants
// is exercised:
//   - CA trust only (no client cert)
//   - CA trust + PEM-style keystore (ssl.keystore.certificate.chain / ssl.keystore.key)
//   - CA trust + legacy keystore (ssl.keystore.location / ssl.key.location)
//
// NewFranzClient does not open a network connection — kgo.NewClient is lazy —
// so the constructor returning nil error is sufficient to prove the option set
// validates.

// tlsFixture writes a self-signed CA-ish cert + key to a t.TempDir() and
// returns (caFile, certFile, keyFile). The same cert is used as both the CA
// and the leaf, which is fine since we never actually establish a TLS
// connection in these tests — we only care that the files parse.
func tlsFixture(t *testing.T) (caFile, certFile, keyFile string) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "kafka-lag-exporter-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	dir := t.TempDir()
	caFile = filepath.Join(dir, "ca.pem")
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	require.NoError(t, os.WriteFile(caFile, certPEM, 0600))
	require.NoError(t, os.WriteFile(certFile, certPEM, 0600))

	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0600))

	return caFile, certFile, keyFile
}

func newFranzClientForTest(t *testing.T, props map[string]string) (*FranzClient, error) {
	t.Helper()
	clusterCfg := config.ClusterConfig{
		Name:               "test",
		BootstrapBrokers:   "127.0.0.1:9092",
		ConsumerProperties: props,
	}
	globalCfg := &config.Config{KafkaClientTimeoutSeconds: 10}
	return NewFranzClient(clusterCfg, globalCfg, noopLogger())
}

func TestNewFranzClient_SSL_TruststoreOnly(t *testing.T) {
	caFile, _, _ := tlsFixture(t)
	c, err := newFranzClientForTest(t, map[string]string{
		"security.protocol":       "SSL",
		"ssl.truststore.location": caFile,
	})
	require.NoError(t, err, "SSL-only config must not collide Dialer + DialTLSConfig")
	require.NotNil(t, c)
	t.Cleanup(c.Close)
}

func TestNewFranzClient_SSL_PEMKeystore(t *testing.T) {
	caFile, certFile, keyFile := tlsFixture(t)
	c, err := newFranzClientForTest(t, map[string]string{
		"security.protocol":              "SSL",
		"ssl.truststore.location":        caFile,
		"ssl.truststore.type":            "PEM",
		"ssl.keystore.type":              "PEM",
		"ssl.keystore.certificate.chain": certFile,
		"ssl.keystore.key":               keyFile,
	})
	require.NoError(t, err, "PEM-style keystore properties must be honored")
	require.NotNil(t, c)
	t.Cleanup(c.Close)
}

func TestNewFranzClient_SSL_LegacyKeystore(t *testing.T) {
	caFile, certFile, keyFile := tlsFixture(t)
	c, err := newFranzClientForTest(t, map[string]string{
		"security.protocol":       "SSL",
		"ssl.truststore.location": caFile,
		"ssl.keystore.location":   certFile,
		"ssl.key.location":        keyFile,
	})
	require.NoError(t, err, "legacy keystore properties must still work")
	require.NotNil(t, c)
	t.Cleanup(c.Close)
}

func TestNewFranzClient_SASL_SSL_Combined(t *testing.T) {
	// SASL_SSL is a common production setup; make sure it also validates.
	caFile, _, _ := tlsFixture(t)
	c, err := newFranzClientForTest(t, map[string]string{
		"security.protocol":       "SASL_SSL",
		"ssl.truststore.location": caFile,
		"sasl.mechanism":          "PLAIN",
		"sasl.username":           "user",
		"sasl.password":           "pass",
	})
	require.NoError(t, err)
	require.NotNil(t, c)
	t.Cleanup(c.Close)
}

func TestBuildTLSConfig_PEMKeystore(t *testing.T) {
	caFile, certFile, keyFile := tlsFixture(t)
	cfg, err := buildTLSConfig(map[string]string{
		"ssl.truststore.location":        caFile,
		"ssl.keystore.certificate.chain": certFile,
		"ssl.keystore.key":               keyFile,
	}, noopLogger())
	require.NoError(t, err)
	require.Len(t, cfg.Certificates, 1, "PEM keystore properties must load a client cert")
	require.NotNil(t, cfg.RootCAs)
}

func TestBuildTLSConfig_PEMKeystore_PreferredOverLegacy(t *testing.T) {
	// When both sets of keys are set, the PEM-style keys win. We detect that
	// indirectly: point the legacy keys at a bogus path and the PEM keys at
	// real files. A clean load proves the legacy path was not consulted.
	_, certFile, keyFile := tlsFixture(t)
	cfg, err := buildTLSConfig(map[string]string{
		"ssl.keystore.certificate.chain": certFile,
		"ssl.keystore.key":               keyFile,
		"ssl.keystore.location":          "/nonexistent/bogus.pem",
		"ssl.key.location":               "/nonexistent/bogus.key",
	}, noopLogger())
	require.NoError(t, err)
	require.Len(t, cfg.Certificates, 1)
}

func TestBuildTLSConfig_AcceptsKeystoreTypePEM(t *testing.T) {
	// ssl.keystore.type / ssl.truststore.type of "PEM" must be accepted
	// without error (the exporter only supports PEM, so the value is
	// informational).
	cfg, err := buildTLSConfig(map[string]string{
		"ssl.keystore.type":   "PEM",
		"ssl.truststore.type": "PEM",
	}, noopLogger())
	require.NoError(t, err)
	require.NotNil(t, cfg)
}

// --- TLSCACert (in-memory CA from Strimzi autodiscovery) tests ---------------

// generateTestCACertPEM returns a PEM-encoded self-signed CA certificate
// suitable for testing the in-memory CA cert path.
func generateTestCACertPEM(t *testing.T) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(42),
		Subject:               pkix.Name{CommonName: "test-strimzi-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes}))
}

func TestNewFranzClient_SSL_TLSCACert_InMemory(t *testing.T) {
	caPEM := generateTestCACertPEM(t)
	clusterCfg := config.ClusterConfig{
		Name:               "strimzi-auto",
		BootstrapBrokers:   "127.0.0.1:9093",
		ConsumerProperties: map[string]string{"security.protocol": "SSL"},
		TLSCACert:          caPEM,
	}
	globalCfg := &config.Config{KafkaClientTimeoutSeconds: 10}
	c, err := NewFranzClient(clusterCfg, globalCfg, noopLogger())
	require.NoError(t, err, "in-memory CA cert from Strimzi should work")
	require.NotNil(t, c)
	t.Cleanup(c.Close)
}

func TestNewFranzClient_SSL_TruststoreFileOverridesTLSCACert(t *testing.T) {
	// When ssl.truststore.location is set (file-based), TLSCACert is ignored.
	// This ensures static config takes precedence over autodiscovery.
	caFile, _, _ := tlsFixture(t)
	caPEM := generateTestCACertPEM(t) // different CA
	clusterCfg := config.ClusterConfig{
		Name:             "mixed",
		BootstrapBrokers: "127.0.0.1:9093",
		ConsumerProperties: map[string]string{
			"security.protocol":       "SSL",
			"ssl.truststore.location": caFile,
		},
		TLSCACert: caPEM,
	}
	globalCfg := &config.Config{KafkaClientTimeoutSeconds: 10}
	c, err := NewFranzClient(clusterCfg, globalCfg, noopLogger())
	require.NoError(t, err, "file-based truststore should take precedence")
	require.NotNil(t, c)
	t.Cleanup(c.Close)
}

func TestBuildTLSConfig_TLSCACert_NotUsedDirectly(t *testing.T) {
	// buildTLSConfig itself doesn't handle TLSCACert — it only deals with
	// properties. The in-memory cert is applied in NewFranzClient. Verify
	// that buildTLSConfig with no truststore.location produces nil RootCAs.
	cfg, err := buildTLSConfig(map[string]string{}, noopLogger())
	require.NoError(t, err)
	assert.Nil(t, cfg.RootCAs, "buildTLSConfig alone should not set RootCAs without truststore.location")
}

func TestClientMetrics_AllCounters(t *testing.T) {
	m := &ClientMetrics{}
	m.OnBrokerConnect(kgo.BrokerMetadata{}, 0, nil, nil)
	m.OnBrokerConnect(kgo.BrokerMetadata{}, 0, nil, nil)
	m.OnBrokerDisconnect(kgo.BrokerMetadata{}, nil)
	m.OnBrokerWrite(kgo.BrokerMetadata{}, 0, 0, 0, 0, assert.AnError)
	m.OnBrokerRead(kgo.BrokerMetadata{}, 0, 0, 0, 0, assert.AnError)
	m.OnBrokerRead(kgo.BrokerMetadata{}, 0, 0, 0, 0, assert.AnError)

	assert.Equal(t, int64(2), m.Connects())
	assert.Equal(t, int64(1), m.Disconnects())
	assert.Equal(t, int64(1), m.WriteErrors())
	assert.Equal(t, int64(2), m.ReadErrors())
}
