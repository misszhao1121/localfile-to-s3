package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Source access policy (hard guarantee, not a convention):
//
// The migration tool touches the source tree with EXACTLY two operations:
//   1. directory listing  (os.ReadDir  -> readdir)
//   2. file open for read (openSourceReadOnly -> open(O_RDONLY) + read + close)
//
// It never calls, anywhere in the codebase: Remove / RemoveAll / Rename /
// Create / OpenFile with write flags / WriteFile / Chmod / Chown / Truncate
// on any path under source_root. The helper below is the single choke point
// for opening source files; it enforces both path containment and O_RDONLY.
//
// If an external process deletes files mid-migration, uploads of those files
// are counted as skipped (done), never retried against a moving target.

// openSourceReadOnly opens path under root strictly for reading.
func openSourceReadOnly(root, path string) (*os.File, error) {
	cleanRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	cleanRoot = filepath.Clean(cleanRoot)

	cleanPath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	cleanPath = filepath.Clean(cleanPath)

	if !withinRoot(cleanRoot, cleanPath) {
		return nil, fmt.Errorf("source access denied: %s is outside source root %s", cleanPath, cleanRoot)
	}
	// O_RDONLY only: no create, no truncate, no append, no write.
	return os.OpenFile(cleanPath, os.O_RDONLY, 0)
}

// withinRoot reports whether path is root itself or strictly inside it.
func withinRoot(root, path string) bool {
	if root == path {
		return true
	}
	if !strings.HasSuffix(root, string(filepath.Separator)) {
		root += string(filepath.Separator)
	}
	return strings.HasPrefix(path, root)
}

// CheckSourceWriteable actively verifies that the source tree REJECTS
// writes (e.g. mounted ro). Returns nil when a probe write fails as
// expected, an explanatory error when the source is writable (warning only,
// migration still runs — the tool itself still never writes).
func CheckSourceWriteable(root string) (rejectsWrites bool, err error) {
	probe := filepath.Join(root, ".rustfs-migrate-ro-probe")
	f, werr := os.OpenFile(probe, os.O_CREATE|os.O_WRONLY, 0o600)
	if werr == nil {
		f.Close()
		os.Remove(probe)
		return false, fmt.Errorf(
			"source root %s accepts writes; for the read-only bottom line, mount it read-only (e.g. 'mount -o bind,ro' or chmod a-w) — this tool itself never writes to the source",
			root)
	}
	return true, nil
}
