package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDefaultDataDir(t *testing.T) {
	tests := []struct {
		name     string
		setupEnv func()
		expected string
	}{
		{
			name: "XDG_DATA_HOME override",
			setupEnv: func() {
				os.Setenv("XDG_DATA_HOME", "/custom/data")
			},
			expected: "/custom/data/flo",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clean up environment
			originalXDG := os.Getenv("XDG_DATA_HOME")
			t.Cleanup(func() {
				if originalXDG != "" {
					os.Setenv("XDG_DATA_HOME", originalXDG)
				} else {
					os.Unsetenv("XDG_DATA_HOME")
				}
			})

			// Set up test environment
			tt.setupEnv()

			result := DefaultDataDir()

			// Check exact match for XDG_DATA_HOME test
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestDefaultDataDirNoHome(t *testing.T) {
	// Test fallback when UserHomeDir fails
	originalHome := os.Getenv("HOME")
	os.Unsetenv("HOME")
	t.Cleanup(func() {
		if originalHome != "" {
			os.Setenv("HOME", originalHome)
		}
	})

	// We can't easily mock UserHomeDir, so we'll test the behavior
	// by ensuring the function doesn't panic and returns a reasonable result
	result := DefaultDataDir()

	// Should return a fallback path
	if result == "" {
		t.Error("Expected non-empty result even when HOME is not set")
	}

	// Should be a reasonable fallback
	if result != "./data" {
		t.Errorf("Expected fallback to './data', got %s", result)
	}
}

func TestIsDir(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected bool
	}{
		{
			name:     "existing directory",
			path:     ".",
			expected: true,
		},
		{
			name:     "non-existent path",
			path:     "/non/existent/path/that/does/not/exist",
			expected: false,
		},
		{
			name:     "file instead of directory",
			path:     os.Args[0], // current executable
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isDir(tt.path)
			if result != tt.expected {
				t.Errorf("isDir(%s) = %v, expected %v", tt.path, result, tt.expected)
			}
		})
	}
}

func TestDefaultDataDirCrossPlatform(t *testing.T) {
	// Test that DefaultDataDir returns a reasonable path on all platforms
	result := DefaultDataDir()

	// Should not be empty
	if result == "" {
		t.Error("DefaultDataDir should not return empty string")
	}

	// Should be an absolute path or start with ./
	if !filepath.IsAbs(result) && !filepath.HasPrefix(result, "./") {
		t.Errorf("DefaultDataDir should return absolute path or start with ./, got %s", result)
	}

	// Should contain "flo" somewhere in the path
	if !strings.HasSuffix(result, "flo") && !strings.HasSuffix(result, "Flo") {
		t.Errorf("DefaultDataDir should contain 'flo' in the path, got %s", result)
	}
}

func TestDefaultDataDirConsistency(t *testing.T) {
	// Test that DefaultDataDir returns the same result when called multiple times
	result1 := DefaultDataDir()
	result2 := DefaultDataDir()

	if result1 != result2 {
		t.Errorf("DefaultDataDir should be consistent, got %s and %s", result1, result2)
	}
}
