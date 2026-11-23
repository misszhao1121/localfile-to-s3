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

// Store is the SQLite-backed task state store. All writes go through
// batched transactions (single writer); readers run concurrently in WAL mode.
type Store struct {
	db *sql.DB
	mu sync.Mutex // serializes write transactions

	batch []PendingRow // pending plan rows buffer
	recs  []RecRow     // result rows buffer

	// counters
	planned  atomic.Int64
	verified atomic.Int64
	uploaded atomic.Int64
	requeued atomic.Int64
	skipped  atomic.Int64
	failed   atomic.Int64
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
// applySchema only runs for migrate mode to guarantee a read-only verify.
func OpenStore(path string, applySchema bool) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	dsn := "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(10000)&_pragma=synchronous(NORMAL)&_pragma=wal_autocheckpoint(64)&_pragma=mmap_size(268435456)&_pragma=cache_size(-262144)&_pragma=foreign_keys(OFF)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// modernc sqlite is fully serialized internally; a single connection
	// avoids SQLITE_BUSY between our own readers/writer.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
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
	return &Store{db: db}, nil
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

func (s *Store) Close() error {
	s.FlushPending()
	s.FlushRecords()
	return s.db.Close()
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

// AddPending buffers a scanned file; FlushPending writes in one transaction.
func (s *Store) AddPending(r PendingRow) {
	s.batch = append(s.batch, r)
	if len(s.batch) >= 4096 {
		s.FlushPending()
	}
}

func (s *Store) FlushPending() {
	if len(s.batch) == 0 {
		return
	}
	rows := s.batch
	s.batch = nil
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		fatalErr("begin plan tx", err)
	}
	stmt, err := tx.Prepare(`INSERT INTO files (key, rel, size, mtime_ns, ino, state) VALUES (?,?,?,?,?,0)
		ON CONFLICT(key) DO UPDATE SET size=excluded.size, mtime_ns=excluded.mtime_ns, ino=excluded.ino,
		state=CASE WHEN files.state=1 THEN 1 ELSE files.state END`)
	if err != nil {
		fatalErr("prepare plan stmt", err)
	}
	for _, r := range rows {
		if _, err := stmt.Exec(r.Rel, r.Rel, r.Sz, r.Mod, int64(r.Ino)); err != nil {
			fatalErr("exec plan stmt", err)
		}
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		fatalErr("commit plan tx", err)
	}
	s.planned.Add(int64(len(rows)))
}

// ---- results (worker -> db) ----

// Record buffers an upload/verify result; FlushRecords writes it in batch.
func (s *Store) Record(r RecRow) {
	s.recs = append(s.recs, r)
	if len(s.recs) >= 2048 {
		s.FlushRecords()
	}
}

func (s *Store) FlushRecords() {
	if len(s.recs) == 0 {
		return
	}
	rows := s.recs
	s.recs = nil
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		fatalErr("begin record tx", err)
	}
	stmt, err := tx.Prepare(`UPDATE files SET
		state=?,
		uploaded_at=CASE WHEN ?=1 THEN ? ELSE uploaded_at END,
		verified_at=CASE WHEN ?=1 THEN ? ELSE verified_at END,
		attempts=attempts+1,
		err_code=CASE WHEN ?=1 THEN '' ELSE ? END,
		err_msg=CASE WHEN ?=1 THEN '' ELSE ? END
		WHERE key=? AND state<>?`)
	if err != nil {
		fatalErr("prepare record stmt", err)
	}
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
			fatalErr("exec record stmt", err)
		}
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		fatalErr("commit record tx", err)
	}
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

// dbCount counts rows in the files table (used for scan summary messages).
func (s *Store) dbCount() (int64, error) {
	var n int64
	err := s.db.QueryRow(`SELECT COUNT(*) FROM files`).Scan(&n)
	return n, err
}

// ---- task fetch ----

// NextPending returns up to n pending keys (state=0), lowest key first.
func (s *Store) NextPending(n int) ([]string, error) {
	rows, err := s.db.Query(`SELECT key FROM files WHERE state=0 ORDER BY key LIMIT ?`, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// NextFailed returns up to n failed keys (state=2), lowest key first.
func (s *Store) NextFailed(n int) ([]string, error) {
	rows, err := s.db.Query(`SELECT key FROM files WHERE state=2 ORDER BY key LIMIT ?`, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

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

// ---- verify support ----

// MarkVerifiedByKey verifies a row only if its recorded size matches the
// remote size; mismatch re-queues it for upload. Returns the action taken.
func (s *Store) MarkVerifiedByKey(key string, remoteSize int64) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var localSize int64
	var state int
	err := s.db.QueryRow(`SELECT size, state FROM files WHERE key=?`, key).Scan(&localSize, &state)
	if errors.Is(err, sql.ErrNoRows) {
		return "absent", nil
	}
	if err != nil {
		return "", err
	}
	if state == StateDone && localSize == remoteSize {
		_, err := s.db.Exec(`UPDATE files SET verified_at=? WHERE key=?`, time.Now().Unix(), key)
		return "ok", err
	}
	if localSize != remoteSize {
		_, err := s.db.Exec(`UPDATE files SET state=0 WHERE key=?`, key)
		return "size-mismatch-requeued", err
	}
	return "ok", nil
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
	s.mu.Lock()
	defer s.mu.Unlock()
	res, err := s.db.Exec(`UPDATE files SET state=0, err_code='', err_msg='' WHERE state=2`)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// ---- scan coordination ----

func (s *Store) GetMeta(k string) (string, error) {
	var v string
	err := s.db.QueryRow(`SELECT v FROM meta WHERE k=?`, k).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return v, err
}

func (s *Store) SetMeta(k, v string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	s.mu.Lock()
	defer s.mu.Unlock()
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

// Snapshot returns live counters plus DB-derived counts.
func (s *Store) Snapshot() Stats {
	st := Stats{
		Planned:  s.planned.Load(),
		Uploaded: s.uploaded.Load(),
		Verified: s.verified.Load(),
		Failed:   s.failed.Load(),
		Skipped:  s.skipped.Load(),
		Requeued: s.requeued.Load(),
	}
	q := func(sqlq string, dst *int64) {
		_ = s.db.QueryRow(sqlq).Scan(dst)
	}
	q(`SELECT COUNT(*) FROM files`, &st.DBTotal)
	q(`SELECT COUNT(*) FROM files WHERE state=1`, &st.DBDone)
	q(`SELECT COUNT(*) FROM files WHERE state=0`, &st.DBPending)
	q(`SELECT COUNT(*) FROM files WHERE state=2`, &st.DBFailed)
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

// WaitForReader is a no-op today because all DB access shares one connection
// guarded by the store mutex for writes; kept for future reader pools.
func (s *Store) WaitForReader() {}
