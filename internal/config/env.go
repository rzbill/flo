package config

import (
	"os"
	"strconv"
	"strings"
)

// FromEnv overlays FLO_* environment variables onto cfg.
func FromEnv(cfg *Config) {
	if v := os.Getenv("FLO_ALLOW_AUTO_CREATE_NAMESPACES"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.AllowAutoCreateNamespaces = b
		}
	}
	if v := os.Getenv("FLO_DEFAULT_NAMESPACE_NAME"); v != "" {
		cfg.DefaultNamespaceName = v
	}
	if v := os.Getenv("FLO_NAMESPACE_NAME_REGEX"); v != "" {
		cfg.NamespaceNameRegex = v
	}
	if v := os.Getenv("FLO_NAMESPACE_DEFAULTS_PARTITIONS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.NamespaceDefaults.Partitions = n
		}
	}
	if v := os.Getenv("FLO_NAMESPACE_DEFAULTS_PAYLOAD_MAX_BYTES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.NamespaceDefaults.PayloadMaxBytes = n
		}
	}
	if v := os.Getenv("FLO_NAMESPACE_DEFAULTS_HEADERS_MAX_BYTES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.NamespaceDefaults.HeadersMaxBytes = n
		}
	}
	if v := os.Getenv("FLO_MAX_NAMESPACES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.MaxNamespaces = n
		}
	}
	if v := os.Getenv("FLO_ALLOWED_NAMESPACES"); v != "" {
		parts := strings.Split(v, ",")
		cfg.AllowedNamespaces = nil
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				cfg.AllowedNamespaces = append(cfg.AllowedNamespaces, p)
			}
		}
	}
}
