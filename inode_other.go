//go:build !linux

package main

import "io/fs"

// fileID is unavailable outside Linux; returns 0.
func fileID(info fs.FileInfo) uint64 { return 0 }
