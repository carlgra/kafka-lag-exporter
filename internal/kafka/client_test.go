package kafka

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
