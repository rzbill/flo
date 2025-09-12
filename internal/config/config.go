package config

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

// Config is the top-level configuration loaded from file/env.
type Config struct {
	AllowAutoCreateNamespaces bool              `json:"allowAutoCreateNamespaces"`
	DefaultNamespaceName      string            `json:"defaultNamespaceName"`
	NamespaceNameRegex        string            `json:"namespaceNameRegex"`
	NamespaceDefaults         NamespaceDefaults `json:"namespaceDefaults"`
	MaxNamespaces             int               `json:"maxNamespaces"`
	AllowedNamespaces         []string          `json:"allowedNamespaces"`
}

// NamespaceDefaults captures per-namespace baseline limits.
type NamespaceDefaults struct {
	Partitions      int `json:"partitions"`
	PayloadMaxBytes int `json:"payloadMaxBytes"`
	HeadersMaxBytes int `json:"headersMaxBytes"`
}

// Default returns built-in defaults.
func Default() Config {
	return Config{
		AllowAutoCreateNamespaces: true,
		DefaultNamespaceName:      "default",
		NamespaceNameRegex:        "[a-z0-9-_]{1,64}",
		NamespaceDefaults: NamespaceDefaults{
			Partitions:      16,
			PayloadMaxBytes: 1 << 20,
			HeadersMaxBytes: 16 << 10,
		},
	}
}

// Load reads configuration from a JSON or YAML file (by extension). If path is empty, returns defaults.
func Load(path string) (Config, error) {
	if path == "" {
		return Default(), nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	cfg := Default()
	ext := filepath.Ext(path)
	switch ext {
	case ".json":
		if err := json.Unmarshal(b, &cfg); err != nil {
			return Config{}, err
		}
	case ".yaml", ".yml":
		// Lazy inline YAML support via json tags using a minimal shim to keep deps light.
		// If YAML is needed now, prefer adding gopkg.in/yaml.v3; for MVP we accept JSON-only.
		return Config{}, errors.New("yaml config not supported yet; use JSON for now")
	default:
		if err := json.Unmarshal(b, &cfg); err != nil {
			return Config{}, err
		}
	}
	return cfg, nil
}
