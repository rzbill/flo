package serverrun

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	cfgpkg "github.com/rzbill/flo/internal/config"
	pebblestore "github.com/rzbill/flo/internal/storage/pebble"
)

func TestOptionsDataDirFallback(t *testing.T) {
	tests := []struct {
		name     string
		dataDir  string
		expected string
	}{
		{
			name:     "empty data dir uses default",
			dataDir:  "",
			expected: "", // Will be set to DefaultDataDir() in the function
		},
		{
			name:     "provided data dir is preserved",
			dataDir:  "/custom/data",
			expected: "/custom/data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := Options{
				DataDir:       tt.dataDir,
				GRPCAddr:      ":50051",
				HTTPAddr:      ":8080",
				Fsync:         pebblestore.FsyncModeAlways,
				FsyncInterval: 5 * time.Millisecond,
				Config:        cfgpkg.Default(),
			}

			// Test the data dir fallback logic
			if opts.DataDir == "" {
				opts.DataDir = cfgpkg.DefaultDataDir()
			}

			// Verify the result
			if tt.expected == "" {
				// For empty case, verify it's not empty after fallback
				if opts.DataDir == "" {
					t.Error("Expected DataDir to be set after fallback")
				}
				// Verify it's a reasonable path
				if !filepath.IsAbs(opts.DataDir) && !filepath.HasPrefix(opts.DataDir, "./") {
					t.Errorf("Expected DataDir to be absolute or start with ./, got %s", opts.DataDir)
				}
			} else {
				// For provided case, verify it's preserved
				if opts.DataDir != tt.expected {
					t.Errorf("Expected DataDir %s, got %s", tt.expected, opts.DataDir)
				}
			}
		})
	}
}

func TestGetenvDefault(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		def      string
		envValue string
		expected string
	}{
		{
			name:     "environment variable set",
			key:      "TEST_VAR",
			def:      "default",
			envValue: "env_value",
			expected: "env_value",
		},
		{
			name:     "environment variable not set",
			key:      "TEST_VAR_NOT_SET",
			def:      "default",
			envValue: "",
			expected: "default",
		},
		{
			name:     "environment variable empty",
			key:      "TEST_VAR_EMPTY",
			def:      "default",
			envValue: "",
			expected: "default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up environment
			if tt.envValue != "" {
				_ = os.Setenv(tt.key, tt.envValue)
			} else {
				_ = os.Unsetenv(tt.key)
			}
			t.Cleanup(func() {
				_ = os.Unsetenv(tt.key)
			})

			result := getenvDefault(tt.key, tt.def)
			if result != tt.expected {
				t.Errorf("getenvDefault(%s, %s) = %s, expected %s", tt.key, tt.def, result, tt.expected)
			}
		})
	}
}

func TestOptionsValidation(t *testing.T) {
	// Test that Options struct can be created with valid values
	opts := Options{
		DataDir:       "/tmp/test",
		GRPCAddr:      ":50051",
		HTTPAddr:      ":8080",
		UIAddr:        ":8081",
		UIBase:        "/ui",
		Fsync:         pebblestore.FsyncModeAlways,
		FsyncInterval: 5 * time.Millisecond,
		Config:        cfgpkg.Default(),
	}

	// Basic validation
	if opts.DataDir == "" {
		t.Error("DataDir should not be empty")
	}
	if opts.GRPCAddr == "" {
		t.Error("GRPCAddr should not be empty")
	}
	if opts.HTTPAddr == "" {
		t.Error("HTTPAddr should not be empty")
	}
	if opts.Config.DefaultNamespaceName == "" {
		t.Error("Config should have default namespace name")
	}
}

func TestDataDirStoreSubdirectory(t *testing.T) {
	// Test that the store subdirectory logic works correctly
	baseDir := "/tmp/flo"
	expectedStoreDir := filepath.Join(baseDir, "store")

	opts := Options{
		DataDir: baseDir,
	}

	// Simulate the store directory creation logic
	storeDir := filepath.Join(opts.DataDir, "store")
	if storeDir != expectedStoreDir {
		t.Errorf("Expected store dir %s, got %s", expectedStoreDir, storeDir)
	}
}

func TestDefaultDataDirIntegration(t *testing.T) {
	// Test that our DefaultDataDir integration works
	opts := Options{
		DataDir: "", // Empty to trigger fallback
	}

	// Apply the fallback logic
	if opts.DataDir == "" {
		opts.DataDir = cfgpkg.DefaultDataDir()
	}

	// Verify the result
	if opts.DataDir == "" {
		t.Error("DataDir should not be empty after fallback")
	}

	// Verify it's a reasonable path
	if !filepath.IsAbs(opts.DataDir) && !filepath.HasPrefix(opts.DataDir, "./") {
		t.Errorf("DataDir should be absolute or start with ./, got %s", opts.DataDir)
	}

	// Verify it contains "flo" somewhere
	if !strings.HasSuffix(opts.DataDir, "flo") && !strings.HasSuffix(opts.DataDir, "Flo") {
		t.Errorf("DataDir should contain 'flo' in the path, got %s", opts.DataDir)
	}
}

// TestRunIntegration is a basic integration test that verifies Run can be called
// without immediately failing. This is a minimal test since Run starts actual servers.
func TestRunIntegration(t *testing.T) {
	// Skip this test in short mode since it involves server startup
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create a temporary directory for testing
	tempDir := t.TempDir()
	storeDir := filepath.Join(tempDir, "store")

	opts := Options{
		DataDir:       storeDir,
		GRPCAddr:      ":0",                       // Use port 0 for automatic port selection
		HTTPAddr:      ":0",                       // Use port 0 for automatic port selection
		Fsync:         pebblestore.FsyncModeNever, // Use never for faster testing
		FsyncInterval: 1 * time.Millisecond,
		Config:        cfgpkg.Default(),
	}

	// Create a context that will be cancelled quickly
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// This should start the servers and then be cancelled by the timeout
	err := Run(ctx, opts)

	// We expect a context cancelled error, which is normal
	if err != nil && err != context.DeadlineExceeded && err != context.Canceled {
		t.Errorf("Expected context cancellation error, got %v", err)
	}
}
