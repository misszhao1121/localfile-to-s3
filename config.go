package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const DefaultConfigPath = "config.conf"

// sourceWriteHint is the message shown when mutable paths collide with the
// read-only source tree.
const sourceWriteHint = "source tree is strictly read-only"

type Config struct {
	SourceRoot     string // e.g. /data/minio/dev-industai-app
	Bucket         string // e.g. dev-industai-app
	Region         string
	Endpoint       string // e.g. http://10.0.0.11:9000 (single LB entry; SDK resolves node IPs)
	AccessKey      string
	SecretKey      string
	StateDir       string
	UploadRoot     string // defaults to StateDir
	Prefix         string // optional fixed prefix prepended to every object key
	Workers        int
	VerifyWorkers  int
	BufferSizeMiB  int
	MaxRetries     int
	RetryBaseMS    int
	RetryMaxMS     int
	ListTokenSize  int
	HTTPTimeoutSec int
	LogFile        string
	RescanOnStart  bool // set scan_done=0: rewalk the tree to pick up new files
	CreateBucket   bool // attempt to create the target bucket if missing
	DedupHead      bool // HEAD before PUT: skip upload when object exists with same size
}

func applyDefaults(c *Config) {
	if c.Region == "" {
		c.Region = "us-east-1"
	}
	if c.StateDir == "" {
		c.StateDir = "./state"
	}
	if c.Workers <= 0 {
		c.Workers = 64
	}
	if c.VerifyWorkers <= 0 {
		c.VerifyWorkers = 64
	}
	if c.BufferSizeMiB <= 0 {
		c.BufferSizeMiB = 4
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = 8
	}
	if c.RetryBaseMS <= 0 {
		c.RetryBaseMS = 250
	}
	if c.RetryMaxMS <= 0 {
		c.RetryMaxMS = 30000
	}
	if c.ListTokenSize <= 0 {
		c.ListTokenSize = 1000
	}
	if c.HTTPTimeoutSec <= 0 {
		c.HTTPTimeoutSec = 60
	}
}

func validate(c *Config) error {
	if c.SourceRoot == "" {
		return fmt.Errorf("source_root is required")
	}
	st, err := os.Stat(c.SourceRoot)
	if err != nil {
		return fmt.Errorf("source_root %s: %w", c.SourceRoot, err)
	}
	if !st.IsDir() {
		return fmt.Errorf("source_root %s is not a directory", c.SourceRoot)
	}
	if c.Bucket == "" {
		return fmt.Errorf("bucket is required")
	}
	if c.Endpoint == "" {
		return fmt.Errorf("endpoint is required (e.g. http://10.0.0.11:9000)")
	}
	if c.AccessKey == "" || c.SecretKey == "" {
		return fmt.Errorf("access_key and secret_key are required")
	}
	if strings.HasPrefix(c.Prefix, "/") {
		return fmt.Errorf("prefix must not start with '/'")
	}
	return nil
}

// LoadConfig reads a flat key = value file. Full-line (# ...) and trailing
// comments are supported; quotes are stripped; keys are case-insensitive.
func LoadConfig(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open config %s: %w", path, err)
	}
	defer f.Close()

	kv := make(map[string]string)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if i := strings.Index(line, "#"); i > 0 && !strings.HasPrefix(line, "http") {
			line = strings.TrimSpace(line[:i])
		}
		eq := strings.Index(line, "=")
		if eq <= 0 {
			return nil, fmt.Errorf("%s:%d invalid line, expect key = value", path, lineNo)
		}
		key := strings.ToLower(strings.TrimSpace(line[:eq]))
		val := strings.TrimSpace(line[eq+1:])
		val = strings.Trim(val, `"'`)
		kv[key] = val
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}

	c := &Config{
		SourceRoot:     kv["source_root"],
		Bucket:         kv["bucket"],
		Region:         kv["region"],
		Endpoint:       kv["endpoint"],
		AccessKey:      kv["access_key"],
		SecretKey:      kv["secret_key"],
		StateDir:       kv["state_dir"],
		UploadRoot:     kv["upload_root"],
		Prefix:         kv["prefix"],
		LogFile:        kv["log_file"],
		ListTokenSize:  getInt(kv, "list_token_size", 0),
		Workers:        getInt(kv, "workers", 0),
		VerifyWorkers:  getInt(kv, "verify_workers", 0),
		BufferSizeMiB:  getInt(kv, "buffer_size_mib", 0),
		MaxRetries:     getInt(kv, "max_retries", 0),
		RetryBaseMS:    getInt(kv, "retry_base_ms", 0),
		RetryMaxMS:     getInt(kv, "retry_max_ms", 0),
		HTTPTimeoutSec: getInt(kv, "http_timeout_sec", 0),
		RescanOnStart:  getBool(kv, "rescan_on_start", false),
		CreateBucket:   getBool(kv, "create_bucket", false),
		DedupHead:      getBool(kv, "dedup_head", true), // default: auto-compare on first run
	}
	applyDefaults(c)
	if err := validate(c); err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	if err := checkReadOnlySeparation(c); err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	return c, nil
}

// checkReadOnlySeparation enforces that all mutable artifacts (state DB,
// logs) live OUTSIDE the source tree: the source is treated as read-only.
func checkReadOnlySeparation(c *Config) error {
	paths := map[string]string{
		"state_dir": c.StateDir,
		"log_file":  c.LogFile,
	}
	for name, p := range paths {
		if p == "" {
			continue
		}
		abs, err := filepath.Abs(p)
		if err != nil {
			continue
		}
		if withinRootAbs(c.SourceRoot, abs) {
			return fmt.Errorf("%s (%s) must be outside source_root %s: %s", name, p, c.SourceRoot, sourceWriteHint)
		}
	}
	return nil
}

// withinRootAbs is withinRoot with independent abs resolution.
func withinRootAbs(root, path string) bool {
	r, err1 := filepath.Abs(root)
	p, err2 := filepath.Abs(path)
	if err1 != nil || err2 != nil {
		return false
	}
	return withinRoot(filepath.Clean(r), filepath.Clean(p))
}

func getInt(kv map[string]string, key string, def int) int {
	if v, ok := kv[key]; ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getBool(kv map[string]string, key string, def bool) bool {
	if v, ok := kv[key]; ok {
		b, err := strconv.ParseBool(v)
		if err == nil {
			return b
		}
	}
	return def
}

func (c *Config) DBPath() string {
	return filepath.Join(c.StateDir, "migrate.db")
}

func (c *Config) ScanDonePath() string {
	return filepath.Join(c.StateDir, "scan.done")
}
