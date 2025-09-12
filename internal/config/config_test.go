package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefault(t *testing.T) {
	cfg := Default()
	if !cfg.AllowAutoCreateNamespaces {
		t.Fatalf("default allow auto create should be true")
	}
	if cfg.DefaultNamespaceName != "default" {
		t.Fatalf("default ns name")
	}
	if cfg.NamespaceDefaults.Partitions != 16 {
		t.Fatalf("partitions default")
	}
}

func TestLoadJSON(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "flo.json")
	data := []byte(`{"allowAutoCreateNamespaces":false,"defaultNamespaceName":"prod","namespaceDefaults":{"partitions":32,"payloadMaxBytes":2048,"headersMaxBytes":1024}}`)
	if err := os.WriteFile(file, data, 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	cfg, err := Load(file)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.AllowAutoCreateNamespaces {
		t.Fatalf("expected false")
	}
	if cfg.DefaultNamespaceName != "prod" {
		t.Fatalf("expected prod")
	}
	if cfg.NamespaceDefaults.Partitions != 32 {
		t.Fatalf("expected 32")
	}
}

func TestFromEnv(t *testing.T) {
	cfg := Default()
	os.Setenv("FLO_ALLOW_AUTO_CREATE_NAMESPACES", "false")
	os.Setenv("FLO_DEFAULT_NAMESPACE_NAME", "staging")
	os.Setenv("FLO_NAMESPACE_DEFAULTS_PARTITIONS", "24")
	t.Cleanup(func() {
		os.Unsetenv("FLO_ALLOW_AUTO_CREATE_NAMESPACES")
		os.Unsetenv("FLO_DEFAULT_NAMESPACE_NAME")
		os.Unsetenv("FLO_NAMESPACE_DEFAULTS_PARTITIONS")
	})
	FromEnv(&cfg)
	if cfg.AllowAutoCreateNamespaces {
		t.Fatalf("env override bool")
	}
	if cfg.DefaultNamespaceName != "staging" {
		t.Fatalf("env override name")
	}
	if cfg.NamespaceDefaults.Partitions != 24 {
		t.Fatalf("env override partitions")
	}
}
