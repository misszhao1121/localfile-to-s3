package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// scanBucket walks the source tree and plans every regular file into the
// store. It is resumable: when meta scan_done=1 it returns immediately.
// Uses limited-depth manual directory listing to keep open FDs low even on
// wide trees.
//
// Source safety: the ONLY filesystem operation on the source tree here is
// os.ReadDir (readdir) plus the dirent metadata already returned by it —
// no open-for-write, no rename, no delete, ever.
func scanBucket(ctx context.Context, cfg *Config, st *Store, prog *Progress) error {
	if st.ScanDone() && !cfg.RescanOnStart {
		n, _ := st.dbCount()
		logf(os.Stdout, "scan: previous scan complete, %d files planned (use rescan_on_start=true to re-walk)\n", n)
		return nil
	}

	root := cfg.SourceRoot
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	root = filepath.Clean(absRoot)

	var dirsOpen atomic.Int64
	var dirsFDLimit = int64(64) // max concurrent open directories

	type dirTask struct {
		abs  string
		rel  string
		depth int
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, 8) // concurrent dir walkers
	var walkErr atomic.Value

	var walk func(dir dirTask)
	walk = func(dir dirTask) {
		defer wg.Done()
		// bound concurrent open directories
		for dirsOpen.Load() >= dirsFDLimit {
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
		dirsOpen.Add(1)
		defer dirsOpen.Add(-1)

		entries, err := os.ReadDir(dir.abs)
		if err != nil {
			walkErr.CompareAndSwap(nil, err)
			return
		}

		var subdirs []dirTask
		for _, e := range entries {
			if ctx.Err() != nil {
				return
			}
			name := e.Name()
			full := filepath.Join(dir.abs, name)
			relKey := name
			if dir.rel != "" {
				relKey = dir.rel + "/" + name
			}
			if e.IsDir() {
				subdirs = append(subdirs, dirTask{abs: full, rel: relKey, depth: dir.depth + 1})
				continue
			}
			// file (or symlink etc.): stat via info
			info, ierr := e.Info()
			if ierr != nil {
				// file vanished mid-scan; skip
				continue
			}
			if !info.Mode().IsRegular() {
				continue
			}
			if strings.HasSuffix(name, ".migrate-tmp") {
				continue
			}
			prog.scanned.Add(1)
			st.AddPending(PendingRow{
				Rel: relKey,
				Sz:  info.Size(),
				Mod: info.ModTime().UnixNano(),
				Ino: fileID(info),
			})
		}

		// spawn subdirs
		for _, sd := range subdirs {
			select {
			case <-ctx.Done():
				return
			default:
			}
			wg.Add(1)
			select {
			case sem <- struct{}{}:
				go func(d dirTask) {
					defer func() { <-sem }()
					walk(d)
				}(sd)
			default:
				// too many concurrent goroutines: do it inline
				walk(sd)
			}
		}
	}

	wg.Add(1)
	walk(dirTask{abs: root, rel: ""})
	wg.Wait()
	st.FlushPending()

	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errv := walkErr.Load(); errv != nil {
		return errv.(error)
	}

	if err := st.SetScanDone(); err != nil {
		return err
	}
	n, _ := st.dbCount()
	logf(os.Stdout, "scan: complete, %d files planned\n", n)
	return nil
}

func fileIDStub() {}

var errNoTasks = errors.New("no tasks")

var _ = errNoTasks
var _ = fileIDStub
