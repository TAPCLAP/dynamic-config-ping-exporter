package main

import (
	"reflect"
	"strings"
	"testing"
	"time"

	pconfig "github.com/czerwonk/ping_exporter/config"
)

func TestParsePingExporterConfigYAML(t *testing.T) {
	t.Parallel()

	t.Run("valid with targets and options", func(t *testing.T) {
		t.Parallel()
		y := `
targets:
  - host: 10.0.0.1
    role: static
dns:
  refresh: 2m
ping:
  history-size: 10
  interval: 3s
  payload-size: 32
  timeout: 500ms
options:
  disableIPv6: false
  disableIPv4: true
`
		cfg, err := parsePingExporterConfigYAML([]byte(y))
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if len(cfg.Targets) != 1 {
			t.Fatalf("targets: got %d want 1", len(cfg.Targets))
		}
		if cfg.Targets[0].Addr != "10.0.0.1" {
			t.Errorf("addr: got %q", cfg.Targets[0].Addr)
		}
		if cfg.Targets[0].Labels["role"] != "static" {
			t.Errorf("labels: %+v", cfg.Targets[0].Labels)
		}
		if cfg.Options.DisableIPv6 || !cfg.Options.DisableIPv4 {
			t.Errorf("options: %+v", cfg.Options)
		}
	})

	t.Run("invalid yaml", func(t *testing.T) {
		t.Parallel()
		_, err := parsePingExporterConfigYAML([]byte("{not yaml"))
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestDefaultPingExporterConfig(t *testing.T) {
	t.Parallel()

	cfg, err := defaultPingExporterConfig()
	if err != nil {
		t.Fatalf("defaultPingExporterConfig: %v", err)
	}

	if len(cfg.Targets) != 0 {
		t.Errorf("targets: want empty slice, got %#v", cfg.Targets)
	}
	if !cfg.Options.DisableIPv6 || cfg.Options.DisableIPv4 {
		t.Errorf("options: want disableIPv6=true disableIPv4=false, got %+v", cfg.Options)
	}

	want := mustParse(t, []byte(strings.TrimSpace(defaultPingExporterConfigYAML)))
	if !reflect.DeepEqual(want, cfg) {
		t.Errorf("default config mismatch\nwant: %#v\ngot:  %#v", want, cfg)
	}
}

func mustParse(t *testing.T, data []byte) pconfig.Config {
	t.Helper()
	cfg, err := parsePingExporterConfigYAML(data)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return cfg
}

func TestParsePingExporterConfigYAML_RoundTripDefault(t *testing.T) {
	t.Parallel()

	cfg1, err := defaultPingExporterConfig()
	if err != nil {
		t.Fatal(err)
	}
	// Re-parse embedded string to ensure stable unmarshaling
	cfg2 := mustParse(t, []byte(strings.TrimSpace(defaultPingExporterConfigYAML)))
	if !reflect.DeepEqual(cfg1, cfg2) {
		t.Errorf("default vs re-parse: %#v vs %#v", cfg1, cfg2)
	}
}

func TestParsePingExporterConfigYAML_EmptyDocument(t *testing.T) {
	t.Parallel()

	cfg, err := parsePingExporterConfigYAML([]byte(""))
	if err != nil {
		t.Fatalf("empty yaml: %v", err)
	}
	if len(cfg.Targets) != 0 {
		t.Errorf("expected no targets, got %d", len(cfg.Targets))
	}
}

// Exercise duration fields unmarshaling (interval uses custom duration type in ping_exporter).
func TestParsePingExporterConfigYAML_Durations(t *testing.T) {
	t.Parallel()

	y := `
targets: []
ping:
  interval: 7s
  timeout: 800ms
dns:
  refresh: 4m
`
	cfg, err := parsePingExporterConfigYAML([]byte(y))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Ping.Interval.Duration(); got != 7*time.Second {
		t.Errorf("Ping.Interval: got %v want %v", got, 7*time.Second)
	}
	if got := cfg.Ping.Timeout.Duration(); got != 800*time.Millisecond {
		t.Errorf("Ping.Timeout: got %v want %v", got, 800*time.Millisecond)
	}
	if got := cfg.DNS.Refresh.Duration(); got != 4*time.Minute {
		t.Errorf("DNS.Refresh: got %v want %v", got, 4*time.Minute)
	}
}
