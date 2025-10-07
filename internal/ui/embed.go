// Package ui contains embedded static assets for the dashboard.
package ui

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed static/**
var assets embed.FS

// FS returns a http.FileSystem for the embedded dashboard assets.
func FS() http.FileSystem {
	sub, err := fs.Sub(assets, "static")
	if err != nil {
		return http.FS(assets)
	}
	return http.FS(sub)
}
