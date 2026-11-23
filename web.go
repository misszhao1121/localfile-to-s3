package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"time"
)

// Job is a managed migration process run (migrate/verify/retry).
// The web server runs at most one job at a time; jobs are spawned as
// in-process goroutine executions of the same run functions used by CLI,
// with a cancellable context, so no external process management is needed.
type Job struct {
	Kind      string    `json:"kind"`   // migrate|verify|retry
	State     string    `json:"state"`  // idle|running|done|failed|canceled
	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at,omitempty"`
	Error     string    `json:"error,omitempty"`

	cancel context.CancelFunc
	mu     sync.Mutex
}

// JobManager owns the single running job slot.
type JobManager struct {
	mu   sync.Mutex
	cur  *Job
	last *Job
}

func (m *JobManager) Current() *Job {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cur
}

func (m *JobManager) Last() *Job {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.last
}

var errJobBusy = fmt.Errorf("another job is already running")

// Start launches a new job of the given kind with the given config path.
func (m *JobManager) Start(kind, cfgPath string) (*Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cur != nil {
		return nil, errJobBusy
	}
	switch kind {
	case string(ModeMigrate), string(ModeVerify), string(ModeRetry):
	default:
		return nil, fmt.Errorf("unknown job kind %q", kind)
	}
	ctx, cancel := context.WithCancel(context.Background())
	j := &Job{Kind: kind, State: "running", StartedAt: time.Now(), cancel: cancel}
	m.cur = j

	go func() {
		cfg, err := LoadConfig(cfgPath)
		var runErr error
		if err != nil {
			runErr = err
		} else {
			st, serr := OpenStore(cfg.DBPath(), j.Kind != string(ModeStats))
			if serr != nil {
				runErr = serr
			} else {
				if err := InitLog(cfg.LogFile); err != nil {
					runErr = err
				} else {
					switch j.Kind {
					case string(ModeMigrate):
						runErr = runMigrate(ctx, cfg, st)
					case string(ModeVerify):
						runErr = runVerify(ctx, cfg, st)
					case string(ModeRetry):
						runErr = runRetry(ctx, cfg, st)
					}
					CloseLog()
				}
				st.Close()
			}
		}
		j.mu.Lock()
		j.EndedAt = time.Now()
		switch {
		case ctx.Err() != nil:
			j.State = "canceled"
		case runErr != nil && runErr != errSilent:
			j.State = "failed"
			j.Error = runErr.Error()
		default:
			j.State = "done"
		}
		j.mu.Unlock()

		m.mu.Lock()
		if m.cur == j {
			m.last = j
			m.cur = nil
		}
		m.mu.Unlock()
	}()
	return j, nil
}

// Stop cancels the running job (graceful drain, same as Ctrl-C).
func (m *JobManager) Stop() bool {
	m.mu.Lock()
	j := m.cur
	m.mu.Unlock()
	if j == nil {
		return false
	}
	j.cancel()
	return true
}

// webServer wires the HTTP API and embedded UI.
type webServer struct {
	cfgPath string
	mgr     *JobManager
	mux     *http.ServeMux
}

func newWebServer(cfgPath string) *webServer {
	ws := &webServer{cfgPath: cfgPath, mgr: &JobManager{}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", ws.handleIndex)
	mux.HandleFunc("GET /api/status", ws.handleStatus)
	mux.HandleFunc("GET /api/stats", ws.handleStats)
	mux.HandleFunc("GET /api/config", ws.handleConfigGet)
	mux.HandleFunc("POST /api/config", ws.handleConfigSave)
	mux.HandleFunc("POST /api/job/{kind}", ws.handleJobStart)
	mux.HandleFunc("POST /api/job/stop", ws.handleJobStop)
	mux.HandleFunc("GET /api/log", ws.handleLogTail)
	ws.mux = mux
	return ws
}

// serve listens on addr; blocks until process exit.
func (ws *webServer) serve(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	logf(os.Stdout, "web UI: http://%s\n", ln.Addr().String())
	return http.Serve(ln, ws.mux)
}
