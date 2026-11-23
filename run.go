package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

// runMigrate: plan (scan) + upload with progress reporting.
func runMigrate(ctx context.Context, cfg *Config, st *Store) error {
	if err := os.MkdirAll(cfg.StateDir, 0o755); err != nil {
		return err
	}
	if err := InitLog(cfg.LogFile); err != nil {
		return fmt.Errorf("open log file: %w", err)
	}
	defer CloseLog()

	prog := NewProgress()

	// pre-flight: reachability and bucket
	cli, err := buildClient(cfg)
	if err != nil {
		return err
	}
	if err := ensureBucket(ctx, cli, cfg); err != nil {
		return err
	}

	logf(os.Stdout, "source : %s (read-only access: list + read only)\n", cfg.SourceRoot)
	logf(os.Stdout, "target : %s -> bucket %s\n", cfg.Endpoint, cfg.Bucket)
	logf(os.Stdout, "state  : %s\n", cfg.DBPath())
	logf(os.Stdout, "workers: %d (dedup_head=%v: existing objects with same size are skipped)\n\n", cfg.Workers, cfg.DedupHead)

	// read-only bottom line: actively confirm the source rejects writes
	if rejects, _ := CheckSourceWriteable(cfg.SourceRoot); !rejects {
		logf(os.Stderr, "WARNING: source root is writable; recommend mounting it read-only. This tool will not write to it regardless.\n")
	}

	// phase 1: scan / plan
	t0 := time.Now()
	if err := scanBucket(ctx, cfg, st, prog); err != nil {
		if errors.Is(err, context.Canceled) {
			logf(os.Stderr, "scan interrupted; state saved, rerun to continue\n")
			return nil
		}
		return err
	}
	scanDur := time.Since(t0).Round(time.Second)

	// phase 2: upload
	prog.SetTotalHint(st.totalCount())
	prog.Start()
	upErr := runUploaders(ctx, cfg, st, prog)
	prog.Stop()

	st.PrintStats()
	logf(os.Stdout, "scan took %s\n", scanDur)
	if errors.Is(upErr, context.Canceled) {
		logf(os.Stderr, "\ninterrupted: state saved. Rerun the same command to resume.\n")
		return nil
	}
	if upErr != nil {
		return upErr
	}
	snap := st.Snapshot()
	if snap.DBFailed > 0 {
		logf(os.Stderr, "\n%d files failed; run 'retry' mode, then 'verify' to confirm.\n", snap.DBFailed)
	}
	logf(os.Stdout, "\nmigration finished: done=%d failed=%d elapsed=%s\n",
		snap.DBDone, snap.DBFailed, time.Since(t0).Round(time.Second))
	return nil
}

// runVerify: HeadObject-based reconciliation with pagination over local state.
// If the local ledger is empty (fresh start / partially migrated by other
// means), the source tree is scanned first so every file gets checked:
// present with matching size -> marked done (no re-upload); missing or
// size-mismatched -> queued for upload.
func runVerify(ctx context.Context, cfg *Config, st *Store) error {
	if err := InitLog(cfg.LogFile); err != nil {
		return fmt.Errorf("open log file: %w", err)
	}
	defer CloseLog()

	cli, err := buildClient(cfg)
	if err != nil {
		return err
	}

	// empty ledger: plan everything first (no upload yet)
	if st.totalCount() == 0 {
		logf(os.Stdout, "ledger empty: scanning source to plan all files for reconciliation...\n")
		prog := NewProgress()
		if err := scanBucket(ctx, cfg, st, prog); err != nil {
			if errors.Is(err, context.Canceled) {
				logf(os.Stderr, "scan interrupted; state saved, rerun to continue\n")
				return nil
			}
			return err
		}
		logf(os.Stdout, "scan: %d files planned\n", st.totalCount())
	}

	prog := NewProgress()
	prog.Start()

	const pageSize = 2000
	pre := keyPrefix(cfg)
	after := ""
verifyLoop:
	for {
		if ctx.Err() != nil {
			break verifyLoop
		}
		rows, err := st.PageRows(after, pageSize)
		if err != nil {
			prog.Stop()
			return err
		}
		if len(rows) == 0 {
			break
		}
		after = rows[len(rows)-1].Key

		var wg sync.WaitGroup
		sem := make(chan struct{}, cfg.VerifyWorkers)
		for _, r := range rows {
			r := r
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				wg.Wait()
				break verifyLoop
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				if ctx.Err() != nil {
					return
				}
				key := pre + r.Key
				exists, size, err := headExists(ctx, cli, cfg.Bucket, key)
				if err != nil {
					// transient HEAD failure: count and retry this row once
					// more before giving up (mock/single-node servers under
					// 64-way HEAD pressure drop keep-alive connections).
					select {
					case <-ctx.Done():
						prog.failed.Add(1)
						return
					case <-time.After(200 * time.Millisecond):
					}
					exists, size, err = headExists(ctx, cli, cfg.Bucket, key)
					if err != nil {
						prog.failed.Add(1)
						return
					}
				}
				if !exists {
					// missing remotely: requeue for upload
					if r.State != StatePending {
						st.Record(RecRow{Key: r.Key, State: StatePending, Code: "verify-missing"})
					} else {
						prog.skipped.Add(1)
					}
					return
				}
				if r.Size != size {
					st.Record(RecRow{Key: r.Key, State: StatePending, Code: "verify-size-mismatch"})
					prog.failed.Add(1)
					return
				}
				if r.State != StateDone {
					// object exists with matching size but local state is behind
					st.Record(RecRow{Key: r.Key, State: StateDone})
				}
				prog.verified.Add(1)
			}()
		}
		wg.Wait()
		st.FlushRecords()
	}
	prog.Stop()

	snap := st.Snapshot()
	logf(os.Stdout, "verify complete: verified=%d missing-requeued=%d pending=%d head-errors=%d\n",
		prog.verified.Load(), snap.DBPending, snap.DBPending, prog.failed.Load())
	if snap.DBPending > 0 {
		logf(os.Stdout, "%d keys need upload; run migrate (or retry) again.\n", snap.DBPending)
	}
	return nil
}

// runRetry: re-attempt failed keys, then immediately upload any pending work.
func runRetry(ctx context.Context, cfg *Config, st *Store) error {
	if err := InitLog(cfg.LogFile); err != nil {
		return fmt.Errorf("open log file: %w", err)
	}
	defer CloseLog()

	// reset failed -> pending so the normal uploader picks them up
	n, err := st.ResetFailedToPending()
	if err != nil {
		return err
	}
	if n > 0 {
		logf(os.Stdout, "requeued %d failed keys as pending\n", n)
	} else {
		logf(os.Stdout, "no failed keys to requeue; uploading remaining pending work\n")
	}

	prog := NewProgress()
	prog.SetTotalHint(st.totalCount())
	prog.Start()
	upErr := runUploaders(ctx, cfg, st, prog)
	prog.Stop()

	snap := st.Snapshot()
	logf(os.Stdout, "retry finished: done=%d failed=%d pending=%d\n",
		snap.DBDone, snap.DBFailed, snap.DBPending)
	if upErr != nil && !errors.Is(upErr, context.Canceled) {
		return upErr
	}
	if snap.DBFailed > 0 {
		logf(os.Stderr, "some keys still fail; first failures:\n")
		st.PrintFailed(10)
		logf(os.Stderr, "check log and cluster health, run retry again.\n")
	}
	return nil
}

// totalCount is the number of rows in the files table (used for progress %).
func (s *Store) totalCount() int64 {
	var n int64
	_ = s.db.QueryRow(`SELECT COUNT(*) FROM files`).Scan(&n)
	return n
}
