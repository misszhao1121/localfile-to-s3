package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// statusJSON is the aggregated /api/status payload.
type statusJSON struct {
	Job       *Job    `json:"job"`
	LastJob   *Job    `json:"last_job,omitempty"`
	Stats     *Stats  `json:"stats,omitempty"`
	DBExists  bool    `json:"db_exists"`
	ConfigOK  bool    `json:"config_ok"`
	ConfigErr string  `json:"config_err,omitempty"`
	SourceOK  bool    `json:"source_ok"`
}

func (ws *webServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	st := &statusJSON{Job: ws.mgr.Current(), LastJob: ws.mgr.Last()}
	if st.Job == nil {
		st.Job = &Job{Kind: "-", State: "idle"}
	}
	cfg, err := LoadConfig(ws.cfgPath)
	if err != nil {
		st.ConfigErr = err.Error()
	} else {
		st.ConfigOK = true
		if _, serr := os.Stat(cfg.SourceRoot); serr == nil {
			st.SourceOK = true
		}
		dbp := cfg.DBPath()
		if _, derr := os.Stat(dbp); derr == nil {
			st.DBExists = true
		}
		if s, serr := openStatsSnapshot(dbp); serr == nil {
			st.Stats = s
		}
	}
	writeJSON(w, st)
}

func (ws *webServer) handleStats(w http.ResponseWriter, r *http.Request) {
	cfg, err := LoadConfig(ws.cfgPath)
	if err != nil {
		httpError(w, http.StatusBadRequest, err.Error())
		return
	}
	s, err := openStatsSnapshot(cfg.DBPath())
	if err != nil {
		httpError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, s)
}

// openStatsSnapshot opens the DB read-only to fetch counts.
func openStatsSnapshot(dbPath string) (*Stats, error) {
	if _, err := os.Stat(dbPath); err != nil {
		return nil, fmt.Errorf("no state db at %s", dbPath)
	}
	st, err := OpenStore(dbPath, false)
	if err != nil {
		return nil, err
	}
	defer st.Close()
	s := st.Snapshot()
	return &s, nil
}

// maskedKeys are config fields whose values must never leave the server.
var maskedKeys = map[string]bool{
	"access_key": true,
	"secret_key": true,
}

// configLine is one parsed config entry for the UI.
type configLine struct {
	Key      string `json:"key"`
	Value    string `json:"value"`
	Masked   bool   `json:"masked"`
	Comment  string `json:"comment,omitempty"` // full-line comment preserved above
	Disabled bool   `json:"disabled,omitempty"`
}

// maskedSentinel is the value the GET API returns for secret fields; on
// save it means "unchanged" and the stored original is preserved.
const maskedSentinel = "\x00MASKED\x00"

func (ws *webServer) handleConfigGet(w http.ResponseWriter, r *http.Request) {
	lines, err := parseConfigFile(ws.cfgPath)
	if err != nil {
		httpError(w, http.StatusNotFound, err.Error())
		return
	}
	out := make([]configLine, 0, len(lines))
	for _, ln := range lines {
		if maskedKeys[ln.Key] {
			ln.Value = maskedSentinel
			ln.Masked = true
		}
		out = append(out, ln)
	}
	writeJSON(w, out)
}

func (ws *webServer) handleConfigSave(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		Lines []configLine `json:"lines"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		httpError(w, http.StatusBadRequest, err.Error())
		return
	}

	// rebuild file content; masked values keep their originals
	existing, err := parseConfigFile(ws.cfgPath)
	if err != nil && !os.IsNotExist(err) {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	orig := map[string]string{}
	for _, ln := range existing {
		if ln.Key != "" {
			orig[ln.Key] = ln.Value
		}
	}

	var b strings.Builder
	for _, ln := range payload.Lines {
		if ln.Key == "" {
			// comment or blank line: pass through verbatim
			b.WriteString(ln.Comment)
			b.WriteString("\n")
			continue
		}
		val := ln.Value
		if maskedKeys[ln.Key] && val == maskedSentinel {
			val = orig[ln.Key] // unchanged secret: keep stored value
		}
		fmt.Fprintf(&b, "%s = %s\n", ln.Key, val)
	}

	if err := os.WriteFile(ws.cfgPath, []byte(b.String()), 0o600); err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	// validate by re-loading
	if _, err := LoadConfig(ws.cfgPath); err != nil {
		httpError(w, http.StatusBadRequest, "saved, but validation failed: "+err.Error())
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

// parseConfigFile reads the config into ordered lines for the UI.
func parseConfigFile(path string) ([]configLine, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []configLine
	for _, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimRight(raw, "\r")
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			out = append(out, configLine{Key: "", Value: "", Comment: line})
			continue
		}
		eq := strings.Index(line, "=")
		if eq <= 0 {
			out = append(out, configLine{Key: "", Value: "", Comment: line})
			continue
		}
		key := strings.ToLower(strings.TrimSpace(line[:eq]))
		val := strings.TrimSpace(line[eq+1:])
		val = strings.Trim(val, `"'`)
		out = append(out, configLine{Key: key, Value: val})
	}
	return out, nil
}

func (ws *webServer) handleJobStart(w http.ResponseWriter, r *http.Request) {
	kind := r.PathValue("kind")
	j, err := ws.mgr.Start(kind, ws.cfgPath)
	if err != nil {
		httpError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, j)
}

func (ws *webServer) handleJobStop(w http.ResponseWriter, r *http.Request) {
	ok := ws.mgr.Stop()
	writeJSON(w, map[string]bool{"stopped": ok})
}

func (ws *webServer) handleLogTail(w http.ResponseWriter, r *http.Request) {
	cfg, err := LoadConfig(ws.cfgPath)
	if err != nil {
		httpError(w, http.StatusBadRequest, err.Error())
		return
	}
	if cfg.LogFile == "" {
		writeJSON(w, map[string]string{"log": "(log_file not configured; output goes to console only)"})
		return
	}
	data, err := os.ReadFile(filepath.Clean(cfg.LogFile))
	if err != nil {
		writeJSON(w, map[string]string{"log": "(no log yet)"})
		return
	}
	text := string(data)
	const maxTail = 64 * 1024
	if len(text) > maxTail {
		text = text[len(text)-maxTail:]
	}
	writeJSON(w, map[string]string{"log": text})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func httpError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// sortedKeys is reserved for future map-key listing needs.
func sortedKeys(m map[string]string) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}
