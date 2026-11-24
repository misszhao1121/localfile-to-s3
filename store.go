package main

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

// Task states.
const (
	StatePending = 0
	StateDone    = 1
	StateFailed  = 2
)

// errSkip signals a task needs no further action because it is already done.
var errSkip = errors.New("skip: already done")

// Store is the SQLite-backed task state store.
//
// Concurrency design (rewritten after a crash on a real 40M-file scan):
//   - AddPending/Record are called from MANY goroutines (parallel scanner
//     walkers, upload workers, verify workers). They only SEND typed
//     messages on buffered channels — no shared slices, no caller locks.
//   - ONE writer goroutine per stream consumes messages in FIFO order and
//     commits size/time-triggered batches inside a single transaction.
//   - Flush* sends an ack-request through the SAME channel, so by FIFO
//     ordering the ack guarantees every previously queued row is committed.
//     It never closes the channel, so it is safe to call any number of
//     times, from anywhere, at any point of the run.
//   - Close flushes both streams, then stops the writers, then closes the DB.
//   - Readers use pooled connections (WAL: concurrent read while writing);
//     in-process write transactions are all inside the writer goroutines.
type Store struct {
	db    *sql.DB
	done  chan struct{}
	closeOnce sync.Once

	planCh   chan planMsg
	recCh    chan recMsg
	planWG   sync.WaitGroup
	recWG    sync.WaitGroup

	// counters
	planned  atomic.Int64
	verified atomic.Int64
	uploaded atomic.Int64
	requeued atomic.Int64
	skipped  atomic.Int64
	failed   atomic.Int64
}

type planMsg struct {
	row PendingRow
	ack chan struct{} // non-nil only for flush requests
}

type recMsg struct {
	row RecRow
	ack chan struct{} // non-nil only for flush requests
}

// PendingRow is one scanned file to plan.
type PendingRow struct {
	Rel string
	Sz  int64
	Mod int64
	Ino uint64
}

// RecRow is one task result to record.
type RecRow struct {
	Key   string
	State int
	Code  string
	Msg   string
}

// OpenStore opens (creating if needed) the state database.
// applySchema only runs for migrate/verify/web modes to guarantee a
// read-only stats mode.
func OpenStore(path string, applySchema bool) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	dsn := "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(30000)&_pragma=synchronous(NORMAL)&_pragma=wal_autocheckpoint(64)&_pragma=mmap_size(268435456)&_pragma=cache_size(-262144)&_pragma=foreign_keys(OFF)&_txlock=immediate"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// WAL allows concurrent readers while a writer runs; our writers are
	// the two goroutines below, readers use pooled connections.
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	db.SetConnMaxIdleTime(0)
	db.SetConnMaxLifetime(0)

	if applySchema {
		if err := storeExec(db, schemaSQL); err != nil {
			db.Close()
			return nil, fmt.Errorf("init schema: %w", err)
		}
	} else {
		var n int
		if err := db.QueryRow(
			"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('files','meta')").Scan(&n); err != nil {
			db.Close()
			return nil, err
		}
		if n < 2 {
			db.Close()
			return nil, fmt.Errorf("state db %s has no schema; run migrate mode first", path)
		}
	}

	s := &Store{
		db:     db,
		done:   make(chan struct{}),
		planCh: make(chan planMsg, 65536),
		recCh:  make(chan recMsg, 32768),
	}
	s.planWG.Add(1)
	go s.planWriter()
	s.recWG.Add(1)
	go s.recWriter()
	return s, nil
}

func storeExec(db *sql.DB, script string) error {
	for _, stmt := range strings.Split(script, ";") {
		var b strings.Builder
		for _, line := range strings.Split(stmt, "\n") {
			t := strings.TrimSpace(line)
			if t == "" || strings.HasPrefix(t, "--") {
				continue
			}
			b.WriteString(t)
			b.WriteString("\n")
		}
		s := strings.TrimSpace(b.String())
		if s == "" {
			continue
		}
		if _, err := db.Exec(s); err != nil {
			return fmt.Errorf("exec %q: %w", firstLine(s), err)
		}
	}
	return nil
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// Close flushes both streams and closes the database. Safe to call once.
func (s *Store) Close() error {
	s.FlushPending()
	s.FlushRecords()
	s.closeOnce.Do(func() { close(s.done) })
	s.planWG.Wait()
	s.recWG.Wait()
	return s.db.Close()
}

// FlushPending commits every previously queued scanned row before returning.
func (s *Store) FlushPending() {
	ack := make(chan struct{})
	s.planCh <- planMsg{ack: ack}
	<-ack
}

// FlushRecords commits every previously queued result before returning.
func (s *Store) FlushRecords() {
	ack := make(chan struct{})
	s.recCh <- recMsg{ack: ack}
	<-ack
}

const schemaSQL = `
-- split intentionally
CREATE TABLE IF NOT EXISTS files (
	key        TEXT PRIMARY KEY,
	rel        TEXT NOT NULL,
	size       INTEGER NOT NULL DEFAULT 0,
	mtime_ns   INTEGER NOT NULL DEFAULT 0,
	ino        INTEGER NOT NULL DEFAULT 0,
	state      INTEGER NOT NULL DEFAULT 0,
	uploaded_at INTEGER NOT NULL DEFAULT 0,
	verified_at INTEGER NOT NULL DEFAULT 0,
	attempts   INTEGER NOT NULL DEFAULT 0,
	err_code   TEXT NOT NULL DEFAULT '',
	err_msg    TEXT NOT NULL DEFAULT ''
);
-- split intentionally
CREATE INDEX IF NOT EXISTS idx_files_state ON files(state, key);
-- split intentionally
CREATE TABLE IF NOT EXISTS meta (
	k TEXT PRIMARY KEY,
	v TEXT NOT NULL
);
`

// ---- plan (scan -> db) ----

// AddPending queues a scanned file; the plan writer commits it in batches.
// When the buffer is full it applies backpressure to the scanner, which is
// correct: the disk is the bottleneck at that point anyway.
func (s *Store) AddPending(r PendingRow) {
	s.planCh <- planMsg{row: r}
}

func (s *Store) planWriter() {
	defer s.planWG.Done()
	const batchMax = 8192
	batch := make([]PendingRow, 0, batchMax)
	t := time.NewTicker(300 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case m := <-s.planCh:
			if m.ack != nil {
				if len(batch) > 0 {
					s.insertPending(batch)
					batch = batch[:0]
				}
				close(m.ack)
				continue
			}
			batch = append(batch, m.row)
			if len(batch) >= batchMax {
				s.insertPending(batch)
				batch = batch[:0]
			}
		case <-t.C:
			if len(batch) > 0 {
				s.insertPending(batch)
				batch = batch[:0]
			}
		case <-s.done:
			return
		}
	}
}

// writeTx runs fn inside one IMMEDIATE transaction with retry on SQLITE_BUSY.
// _txlock=immediate in the DSN makes Begin take the write lock up front, so
// busy_timeout actually queues instead of failing fast on lock upgrades.
// Writers never os.Exit on transient DB errors: they retry, then report.
func (s *Store) writeTx(what string, fn func(tx *sql.Tx) error) {
	const maxAttempts = 5
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(100*(1<<attempt)) * time.Millisecond) // 200ms..3.2s
		}
		tx, err := s.db.Begin()
		if err != nil {
			lastErr = err
			logErrf("db %s begin attempt %d failed: %v\n", what, attempt+1, err)
			continue
		}
		if err := fn(tx); err != nil {
			tx.Rollback()
			lastErr = err
			logErrf("db %s tx attempt %d failed: %v\n", what, attempt+1, err)
			continue
		}
		if err := tx.Commit(); err != nil {
			lastErr = err
			logErrf("db %s commit attempt %d failed: %v\n", what, attempt+1, err)
			continue
		}
		return // success
	}
	// persistent DB failure: stop the process with a clear reason
	fatalErr("db writes failed repeatedly ("+what+")", lastErr)
}

func (s *Store) insertPending(rows []PendingRow) {
	s.writeTx("plan", func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(`INSERT INTO files (key, rel, size, mtime_ns, ino, state) VALUES (?,?,?,?,?,0)
			ON CONFLICT(key) DO UPDATE SET size=excluded.size, mtime_ns=excluded.mtime_ns, ino=excluded.ino,
			state=CASE WHEN files.state=1 THEN 1 ELSE files.state END`)
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, r := range rows {
			if _, err := stmt.Exec(r.Rel, r.Rel, r.Sz, r.Mod, int64(r.Ino)); err != nil {
				return err
			}
		}
		return nil
	})
	s.planned.Add(int64(len(rows)))
}

// ---- results (worker -> db) ----

// Record queues an upload/verify result; the record writer commits batches.
func (s *Store) Record(r RecRow) {
	s.recCh <- recMsg{row: r}
}

func (s *Store) recWriter() {
	defer s.recWG.Done()
	const batchMax = 4096
	batch := make([]RecRow, 0, batchMax)
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case m := <-s.recCh:
			if m.ack != nil {
				if len(batch) > 0 {
					s.applyRecords(batch)
					batch = batch[:0]
				}
				close(m.ack)
				continue
			}
			batch = append(batch, m.row)
			if len(batch) >= batchMax {
				s.applyRecords(batch)
				batch = batch[:0]
			}
		case <-t.C:
			if len(batch) > 0 {
				s.applyRecords(batch)
				batch = batch[:0]
			}
		case <-s.done:
			return
		}
	}
}

func (s *Store) applyRecords(rows []RecRow) {
	s.writeTx("record", func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(`UPDATE files SET
			state=?,
			uploaded_at=CASE WHEN ?=1 THEN ? ELSE uploaded_at END,
			verified_at=CASE WHEN ?=1 THEN ? ELSE verified_at END,
			attempts=attempts+1,
			err_code=CASE WHEN ?=1 THEN '' ELSE ? END,
			err_msg=CASE WHEN ?=1 THEN '' ELSE ? END
			WHERE key=? AND state<>?`)
		if err != nil {
			return err
		}
		defer stmt.Close()
		now := time.Now().Unix()
		for _, r := range rows {
			// r.State: 1=done, 2=failed, 0=requeue(pending)
			code, msg := "", ""
			if r.State != 1 {
				code, msg = r.Code, truncateMsg(r.Msg)
			}
			if _, err := stmt.Exec(
				r.State, r.State, now, r.State, now,
				r.State, code, r.State, msg, r.Key, r.State,
			); err != nil {
				return err
			}
		}
		return nil
	})
	for _, r := range rows {
		switch r.State {
		case 1:
			s.uploaded.Add(1)
		case 2:
			s.failed.Add(1)
		case 0:
			s.requeued.Add(1)
		}
	}
}

func truncateMsg(m string) string {
	const max = 512
	if len(m) > max {
		return m[:max]
	}
	return m
}

// ---- task fetch ----

// MetaTask is a pending or failed row with its source path info.
type MetaTask struct {
	Key     string
	RelPath string
	Size    int64
}

// NextMetaPending returns up to n rows in the given state with metadata,
// ordered by key and strictly after `after` (cursor pagination).
func (s *Store) NextMetaPending(state int, n int, after string) ([]MetaTask, error) {
	rows, err := s.db.Query(
		`SELECT key, rel, size FROM files WHERE state=? AND key > ? ORDER BY key LIMIT ?`,
		state, after, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MetaTask
	for rows.Next() {
		var t MetaTask
		if err := rows.Scan(&t.Key, &t.RelPath, &t.Size); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// PageRows returns rows ordered by key strictly after `after`, for pagination.
type PageRow struct {
	Key   string
	Rel   string
	Size  int64
	State int
}

func (s *Store) PageRows(after string, limit int) ([]PageRow, error) {
	rows, err := s.db.Query(
		`SELECT key, rel, size, state FROM files WHERE key > ? ORDER BY key LIMIT ?`,
		after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PageRow
	for rows.Next() {
		var r PageRow
		if err := rows.Scan(&r.Key, &r.Rel, &r.Size, &r.State); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ResetFailedToPending requeues failed rows; returns the number requeued.
func (s *Store) ResetFailedToPending() (int64, error) {
	res, err := s.db.Exec(`UPDATE files SET state=0, err_code='', err_msg='' WHERE state=2`)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// ---- scan coordination (meta) ----

func (s *Store) GetMeta(k string) (string, error) {
	var v string
	err := s.db.QueryRow(`SELECT v FROM meta WHERE k=?`, k).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return v, err
}

func (s *Store) SetMeta(k, v string) error {
	_, err := s.db.Exec(`INSERT INTO meta(k,v) VALUES(?,?)
		ON CONFLICT(k) DO UPDATE SET v=excluded.v`, k, v)
	return err
}

// ScanDone returns whether the previous scan finished.
func (s *Store) ScanDone() bool {
	v, _ := s.GetMeta("scan_done")
	return v == "1"
}

func (s *Store) SetScanDone() error { return s.SetMeta("scan_done", "1") }

// Rescan rewinds the scan_done flag so the next run walks the tree again.
func (s *Store) Rescan() error { return s.SetMeta("scan_done", "0") }

// Reset clears all task state.
func (s *Store) Reset() error {
	for _, q := range []string{`DELETE FROM files`, `DELETE FROM meta`} {
		if _, err := s.db.Exec(q); err != nil {
			return err
		}
	}
	return nil
}

// Stats is a point-in-time snapshot of counters.
type Stats struct {
	Planned, Uploaded, Verified, Failed, Skipped, Requeued int64
	DBTotal, DBDone, DBPending, DBFailed                   int64
}

// Snapshot returns live counters plus DB-derived counts (one grouped query).
func (s *Store) Snapshot() Stats {
	st := Stats{
		Planned:  s.planned.Load(),
		Uploaded: s.uploaded.Load(),
		Verified: s.verified.Load(),
		Failed:   s.failed.Load(),
		Skipped:  s.skipped.Load(),
		Requeued: s.requeued.Load(),
	}
	rows, err := s.db.Query(`SELECT state, COUNT(*) FROM files GROUP BY state`)
	if err == nil {
		for rows.Next() {
			var state, cnt int64
			if rows.Scan(&state, &cnt) == nil {
				st.DBTotal += cnt
				switch state {
				case StateDone:
					st.DBDone = cnt
				case StatePending:
					st.DBPending = cnt
				case StateFailed:
					st.DBFailed = cnt
				}
			}
		}
		rows.Close()
	}
	return st
}

// PrintStats prints the snapshot to stdout.
func (s *Store) PrintStats() {
	t := s.Snapshot()
	fmt.Printf("scanned/planned : %d\n", t.Planned)
	fmt.Printf("done (db)       : %d\n", t.DBDone)
	fmt.Printf("pending (db)    : %d\n", t.DBPending)
	fmt.Printf("failed (db)     : %d\n", t.DBFailed)
	fmt.Printf("total (db)      : %d\n", t.DBTotal)
	fmt.Printf("uploaded (run)  : %d\n", t.Uploaded)
	fmt.Printf("verified (run)  : %d\n", t.Verified)
	fmt.Printf("requeued (run)  : %d\n", t.Requeued)
	fmt.Printf("failed  (run)   : %d\n", t.Failed)
}

// dbCount counts rows in the files table (used for scan summary messages).
func (s *Store) dbCount() (int64, error) {
	var n int64
	err := s.db.QueryRow(`SELECT COUNT(*) FROM files`).Scan(&n)
	return n, err
}

// PrintFailed prints up to n failed rows with their error messages.
func (s *Store) PrintFailed(n int) {
	rows, err := s.db.Query(
		`SELECT key, attempts, err_code, err_msg FROM files WHERE state=2 ORDER BY key LIMIT ?`, n)
	if err != nil {
		fmt.Println("query failed rows:", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var key, code, msg string
		var attempts int
		_ = rows.Scan(&key, &attempts, &code, &msg)
		fmt.Printf("FAILED %s attempts=%d code=%s\n  %s\n", key, attempts, code, msg)
	}
}
