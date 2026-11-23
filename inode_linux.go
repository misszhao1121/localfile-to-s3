//go:build linux

package main

import (
	"io/fs"
	"syscall"
)

// fileID returns the inode number on Linux for scan dedup diagnostics.
func fileID(info fs.FileInfo) uint64 {
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		return uint64(st.Ino)
	}
	return 0
}
