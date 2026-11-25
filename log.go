package main

import (
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var (
	logMu   sync.Mutex
	logFile *os.File
)

// InitLog opens the log file (append) if configured.
func InitLog(path string) error {
	if path == "" {
		return nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	logFile = f
	return nil
}

func CloseLog() {
	logMu.Lock()
	defer logMu.Unlock()
	if logFile != nil {
		logFile.Close()
		logFile = nil
	}
}

// logf writes to both stdout (or stderr) and the log file if present.
func logf(w io.Writer, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	logMu.Lock()
	defer logMu.Unlock()
	fmt.Fprint(w, msg)
	if logFile != nil {
		fmt.Fprint(logFile, msg)
	}
}

// logErrf writes an error line to stderr and the log file.
func logErrf(format string, args ...any) {
	logf(os.Stderr, format, args...)
}

// fatalErr logs and terminates the process with a non-zero code.
func fatalErr(what string, err error) {
	logErrf("FATAL %s: %v\n", what, err)
	os.Exit(2)
}

// ---- progress reporter ----

type Progress struct {
	mu        sync.Mutex
	start     time.Time
	lastLine  string
	stop      chan struct{}
	stopped   chan struct{}
	uploaded  atomic.Int64
	verified  atomic.Int64
	failed    atomic.Int64
	skipped   atomic.Int64
	bytesUp   atomic.Int64
	scanned   atomic.Int64
	inFlight  atomic.Int64
	totalHint atomic.Int64
	retried   atomic.Int64 // retry events absorbed (503/SlowDown/net errors)
	headErr   atomic.Int64 // dedup-HEAD failures (upload proceeded anyway)
	lim       limiterView  // adaptive in-flight cap (nil until set)
}

func NewProgress() *Progress {
	return &Progress{
		start:   time.Now(),
		stop:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
}

func (p *Progress) Add(field *atomic.Int64, n int64) { field.Add(n) }

func (p *Progress) SetTotalHint(n int64) { p.totalHint.Store(n) }

// limiterView is the minimal surface Progress needs from the limiter.
type limiterView interface{ Limit() int }

func (p *Progress) SetLimit(l limiterView) {
	p.mu.Lock()
	p.lim = l
	p.mu.Unlock()
}

func (p *Progress) IncInFlight(d int64) { p.inFlight.Add(d) }

// currentLimit reads the adaptive cap; 0 when not set.
func (p *Progress) currentLimit() int {
	p.mu.Lock()
	l := p.lim
	p.mu.Unlock()
	if l == nil {
		return 0
	}
	return l.Limit()
}

func (p *Progress) Start() {
	go func() {
		defer close(p.stopped)
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		p.render(true)
		for {
			select {
			case <-p.stop:
				p.render(false)
				fmt.Println()
				return
			case <-t.C:
				p.render(false)
			}
		}
	}()
}

func (p *Progress) Stop() {
	close(p.stop)
	<-p.stopped
}

func (p *Progress) render(forceNL bool) {
	elapsed := time.Since(p.start).Round(time.Second)
	up := p.uploaded.Load()
	vf := p.verified.Load()
	boot := p.skipped.Load()
	fail := p.failed.Load()
	sc := p.scanned.Load()
	inF := p.inFlight.Load()
	total := p.totalHint.Load()

	rate := float64(0)
	if elapsed > 0 {
		rate = float64(up) / elapsed.Seconds()
	}
	var bps float64
	if elapsed > 0 {
		bps = float64(p.bytesUp.Load()) / elapsed.Seconds()
	}

	var done int64
	if total > 0 {
		done = up + boot
	}

	line := fmt.Sprintf("[%s] scanned=%s uploaded=%s verified=%s resumed-done=%s failed=%s inflight=%d/%d retry=%s rate=%.0f obj/s %.1f MB/s",
		elapsed,
		comma(sc), comma(up), comma(vf), comma(boot), comma(fail),
		inF, p.currentLimit(), comma(p.retried.Load()), rate, bps/1024/1024)
	if total > 0 {
		pct := float64(done) / float64(total) * 100
		line += fmt.Sprintf(" progress=%.1f%% (%s/%s)", pct, comma(done), comma(total))
	}

	logMu.Lock()
	prev := p.lastLine
	if forceNL && prev != "" {
		fmt.Println()
	}
	// carriage-return progress line
	pad := len(prev) - len(line)
	if pad < 0 {
		pad = 0
	}
	fmt.Printf("\r%-*s", len(line)+pad, line)
	p.lastLine = line
	logMu.Unlock()
}

func comma(n int64) string {
	s := fmt.Sprintf("%d", n)
	if n < 1000 {
		return s
	}
	var b []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			b = append(b, ',')
		}
		b = append(b, c)
	}
	return string(b)
}
