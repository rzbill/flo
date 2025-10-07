package config

import (
	"os"
	"path/filepath"
)

// DefaultDataDir returns the default data directory based on the host OS.
// It prefers standard locations when available and falls back to a dotdir
// in the user's home directory.
func DefaultDataDir() string {
	homeDir, err := os.UserHomeDir()
	if err != nil || homeDir == "" {
		return "./data"
	}

	// XDG (Linux) override
	if xdg := os.Getenv("XDG_DATA_HOME"); xdg != "" {
		return filepath.Join(xdg, "flo")
	}

	// Common Linux/Unix system dir
	if isDir("/var/lib") {
		return "/var/lib/flo"
	}

	// macOS: ~/Library/Application Support/Flo
	if isDir(filepath.Join(homeDir, "Library")) {
		return filepath.Join(homeDir, "Library", "Application Support", "Flo")
	}

	// Windows: %USERPROFILE%/AppData/Local/Flo
	if isDir(filepath.Join(homeDir, "AppData")) {
		return filepath.Join(homeDir, "AppData", "Local", "Flo")
	}

	// Fallback: ~/.flo
	return filepath.Join(homeDir, ".flo")
}

func isDir(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.IsDir()
}
