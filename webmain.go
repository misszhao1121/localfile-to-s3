package main

import (
	"os"
)

// runWeb starts the web UI server. Config must exist and validate; the UI
// can then edit and re-save it.
func runWeb(cfg *Config, cfgPath string) error {
	addr := os.Getenv("RUSTFS_MIGRATE_WEB_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	ws := newWebServer(cfgPath)
	logf(os.Stdout, "rustfs-migrate web UI\n")
	logf(os.Stdout, "config : %s\n", cfgPath)
	logf(os.Stdout, "source : %s\n", cfg.SourceRoot)
	return ws.serve(addr)
}
