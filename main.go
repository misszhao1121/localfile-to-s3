// rustfs-migrate: migrate tens of millions of small files from a local
// MinIO bucket directory into an S3-compatible cluster (RustFS).
//
// Design highlights for 40M small files:
//   - Interruptible scan: directory walk is resumable; a done-marker file
//     skips the whole scan on restart.
//   - SQLite state store: every pending/done file is planned and recorded in
//     batched transactions, crash-safe with WAL.
//   - Resume by default: pending tasks resumed; --verify uses HeadObject to
//     check what actually landed on RustFS; --resume-from-scratch re-plans.
//   - Upload path: parallel GetObject -> shared buffer pool -> PutObject,
//     with prefix-bucketed shuffle to avoid interleaving directory streams.
//   - File descriptor caps: limited depth-first listing keeps open dirs low.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

var errSilent = errors.New("silent")

func main() {
	if err := run(); err != nil {
		if err == errSilent {
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var cfgPath string
	args := os.Args[1:]
	positional := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch a {
		case "-h", "--help", "help":
			usage()
			return nil
		case "-v", "--version":
			fmt.Println("rustfs-migrate 1.2.0")
			return nil
		case "-c", "--config":
			if i+1 >= len(args) {
				return fmt.Errorf("%s requires a file path", a)
			}
			i++
			cfgPath = args[i]
		default:
			positional = append(positional, a)
		}
	}

	mode := ModeMigrate
	if len(positional) > 0 {
		switch positional[0] {
		case string(ModeMigrate):
			mode = ModeMigrate
		case string(ModeVerify):
			mode = ModeVerify
		case string(ModeRetry):
			mode = ModeRetry
		case string(ModeStats):
			mode = ModeStats
		case string(ModeReset):
			mode = ModeReset
		case string(ModeWeb):
			mode = ModeWeb
		default:
			cfgPath = positional[0]
		}
	}

	if cfgPath == "" {
		env := os.Getenv("RUSTFS_MIGRATE_CONFIG")
		if env != "" {
			cfgPath = env
		} else {
			cfgPath = DefaultConfigPath
		}
	}

	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		return err
	}

	st, err := OpenStore(cfg.DBPath(), mode == ModeMigrate || mode == ModeVerify || mode == ModeWeb)
	if err != nil {
		return fmt.Errorf("open state db %s: %w", cfg.DBPath(), err)
	}
	defer st.Close()

	ctx, cancel := context.WithCancel(context.Background())
	sig := make(chan os.Signal, 2)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		fmt.Fprintln(os.Stderr, "SIGINT/SIGTERM received: draining in-flight uploads; press again to force stop")
		cancel()
		<-sig
		fmt.Fprintln(os.Stderr, "forced stop")
		os.Exit(130)
	}()
	defer cancel()

	switch mode {
	case ModeMigrate:
		return runMigrate(ctx, cfg, st)
	case ModeVerify:
		return runVerify(ctx, cfg, st)
	case ModeRetry:
		return runRetry(ctx, cfg, st)
	case ModeStats:
		st.PrintStats()
		return nil
	case ModeReset:
		return st.Reset()
	case ModeWeb:
		st.Close() // web manages its own store per job
		return runWeb(cfg, cfgPath)
	}
	return nil
}

func usage() {
	fmt.Print(`rustfs-migrate - migrate a local MinIO bucket directory to an S3-compatible cluster

Usage:
  rustfs-migrate [-c config.conf] [mode]
  RUSTFS_MIGRATE_CONFIG=/path/config.conf rustfs-migrate [mode]

Modes:
  (default)          plan + upload
  web                web UI: config editor, start/stop, live progress (port 8080)
  verify             reconcile local state with HeadObject on the cluster
  retry              re-attempt failed keys
  stats              print counts
  reset              clear all task state (keeps config)

Examples:
  rustfs-migrate -c /etc/rustfs-migrate/config.conf
  rustfs-migrate -c config.conf verify
  rustfs-migrate -c config.conf retry
  rustfs-migrate -c config.conf web

During the run: Ctrl-C drains in-flight uploads and saves state; press twice to force stop.
`)
}
