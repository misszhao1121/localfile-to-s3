package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithy "github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// buildClient creates an S3 client tuned for many small PUTs.
func buildClient(cfg *Config) (*s3.Client, error) {
	var opts []func(*config.LoadOptions) error
	opts = append(opts,
		config.WithRegion(cfg.Region),
		config.WithCredentialsProvider(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     cfg.AccessKey,
				SecretAccessKey: cfg.SecretKey,
			}, nil
		})),
	)
	awsCfg, err := config.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, err
	}

	transport := &http.Transport{
		Proxy:               nil, // cluster-internal traffic: no proxy
		MaxIdleConns:        cfg.Workers * 2,
		MaxIdleConnsPerHost: cfg.Workers * 2,
		MaxConnsPerHost:     cfg.Workers * 2,
		IdleConnTimeout:     90 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2: false, // small PUTs: avoid HTTP/2 stream caps
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
		o.UsePathStyle = true
		o.HTTPClient = &http.Client{
			Transport: transport,
			Timeout:   time.Duration(cfg.HTTPTimeoutSec) * time.Second,
		}
	})
	return client, nil
}

// keyPrefix returns the configured fixed prefix ("" if none).
func keyPrefix(cfg *Config) string {
	p := cfg.Prefix
	if p != "" && !strings.HasSuffix(p, "/") {
		p += "/"
	}
	return p
}

// taskKey is one unit of upload work.
type taskKey struct {
	Key  string // full object key (prefix applied)
	Path string // absolute source path
	Size int64
	From int // source of the batch: 0=pending, 2=failed
}

// fetchTasks pulls a work batch from the DB after cursor `after`, optionally
// shuffling by key prefix to avoid interleaving many directory streams on the
// same source disk. It returns the next cursor: the largest DB-ordered key of
// the page (captured before shuffling, which reorders the slice).
func fetchTasks(st *Store, cfg *Config, state int, n int, after string, shuffle bool) ([]taskKey, string, error) {
	rows, err := st.NextMetaPending(state, n, after)
	if err != nil {
		return nil, "", err
	}
	if len(rows) == 0 {
		return nil, after, nil
	}
	next := rows[len(rows)-1].Key // DB order = key order; safe cursor
	pre := keyPrefix(cfg)
	out := make([]taskKey, 0, len(rows))
	for _, r := range rows {
		out = append(out, taskKey{
			Key:  pre + r.Key,
			Path: filepath.Join(cfg.SourceRoot, r.RelPath),
			Size: r.Size,
			From: state,
		})
	}
	if shuffle {
		shuffleByPrefix(out)
	}
	return out, next, nil
}

// shuffleByPrefix groups by first path segment, then interleaves the groups
// round-robin so concurrent workers read from different directories.
func shuffleByPrefix(tasks []taskKey) {
	buckets := make(map[string][]taskKey)
	for _, t := range tasks {
		k := t.Key
		if i := strings.IndexByte(k, '/'); i >= 0 {
			k = k[:i]
		}
		buckets[k] = append(buckets[k], t)
	}
	keys := make([]string, 0, len(buckets))
	for k := range buckets {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	idx := make([]int, len(keys))
	out := tasks[:0]
	remaining := len(tasks)
	for remaining > 0 {
		for gi, g := range keys {
			if idx[gi] >= len(buckets[g]) {
				continue
			}
			out = append(out, buckets[g][idx[gi]])
			idx[gi]++
			remaining--
		}
	}
}

// retryable reports whether an S3 error should be retried.
func retryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var re *smithyhttp.ResponseError
	if errors.As(err, &re) {
		code := re.HTTPStatusCode()
		switch code {
		case http.StatusInternalServerError,
			http.StatusBadGateway,
			http.StatusServiceUnavailable,
			http.StatusGatewayTimeout,
			http.StatusInsufficientStorage,
			http.StatusTooManyRequests:
			return true
		}
		if code == 400 {
			// some S3-compatible servers use 400 for slow-down
			var ae smithy.APIError
			if errors.As(err, &ae) && (ae.ErrorCode() == "SlowDown" || ae.ErrorCode() == "RequestTimeout") {
				return true
			}
		}
		return false
	}
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "SlowDown", "RequestTimeout", "ThrottlingException", "InternalError", "ServiceUnavailable", "503":
			return true
		}
		return false
	}
	// network-level errors (connection reset, EOF, timeout) are retryable
	var ne net.Error
	if errors.As(err, &ne) {
		return true
	}
	return true // unknown transport-level error: attempt retry
}

func backoff(base, maxMs int, attempt int) time.Duration {
	d := time.Duration(base) * time.Millisecond
	for i := 1; i < attempt && d < time.Duration(maxMs)*time.Millisecond; i++ {
		d *= 2
	}
	if d > time.Duration(maxMs)*time.Millisecond {
		d = time.Duration(maxMs) * time.Millisecond
	}
	// jitter ±20%
	j := d / 5
	return d - j + time.Duration(2*int64(j)*int64(time.Now().UnixNano()%1e9)/1e9)
}

// uploadOne uploads one task with retries. When dedup_head is enabled the
// first attempt starts with a HeadObject: an object already present with the
// same size (e.g. uploaded previously by other means) is marked done without
// any PUT traffic. Returns (uploaded, skipped, error) — exactly one of
// uploaded/skipped is true on success.
func uploadOne(ctx context.Context, cli *s3.Client, cfg *Config, pool *sync.Pool, prog *Progress, t taskKey) (bool, bool, error) {
	// first-run auto-compare: skip files that already exist remotely
	if cfg.DedupHead {
		exists, size, err := headExists(ctx, cli, cfg.Bucket, t.Key)
		if err != nil {
			// HEAD failure must not block the upload: fall through to PUT
			logErrf("dedup head failed for %s (will upload anyway): %v\n", t.Key, err)
		} else if exists && size == t.Size {
			return false, true, nil
		}
	}
	var lastErr error
	for attempt := 0; attempt < cfg.MaxRetries; attempt++ {
		if ctx.Err() != nil {
			return false, false, ctx.Err()
		}
		if attempt > 0 {
			time.Sleep(backoff(cfg.RetryBaseMS, cfg.RetryMaxMS, attempt))
		}
		done, err := putOnce(ctx, cli, cfg, pool, prog, t)
		if err == nil {
			return done, false, nil
		}
		if errors.Is(err, errSkip) {
			return true, false, nil
		}
		if !retryable(err) {
			return false, false, err
		}
		lastErr = err
	}
	return false, false, lastErr
}

// putOnce performs a single attempt: read the file (pooled buffer) and PUT
// it as one object. Small files are the dominant case, so the whole object
// is buffered in memory to produce a seekable body — required by the SDK's
// CRC32 trailing checksum on plain HTTP endpoints.
//
// Source access is strictly read-only: the file is opened through
// openSourceReadOnly (O_RDONLY, path containment enforced).
// Returns done=true with skipped already counted for vanished files.
func putOnce(ctx context.Context, cli *s3.Client, cfg *Config, pool *sync.Pool, prog *Progress, t taskKey) (bool, error) {
	f, err := openSourceReadOnly(cfg.SourceRoot, t.Path)
	if err != nil {
		if os.IsNotExist(err) {
			// file deleted at source since scan: done as skipped
			return true, nil
		}
		return false, fmt.Errorf("open %s: %w", t.Path, err)
	}
	defer f.Close()

	bufp := pool.Get().(*[]byte)
	buf := (*bufp)[:0]
	defer func() {
		*bufp = buf[:0:cap(*bufp)]
		pool.Put(bufp)
	}()
	// capacity must exceed the declared size; fall back to exact alloc
	// for oversized files beyond the pooled buffer.
	if int64(cap(buf)) < t.Size {
		buf = make([]byte, 0, t.Size)
	}
	buf, err = readAllInto(buf, f, t.Size)
	if err != nil {
		return false, fmt.Errorf("read %s: %w", t.Path, err)
	}

	_, err = cli.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(cfg.Bucket),
		Key:           aws.String(t.Key),
		Body:          bytes.NewReader(buf),
		ContentLength: aws.Int64(int64(len(buf))),
	})
	if err != nil {
		return false, fmt.Errorf("put %s: %w", t.Key, err)
	}
	prog.bytesUp.Add(int64(len(buf)))
	return true, nil
}

// readAllInto appends the whole file to buf, guarding against size drift.
func readAllInto(buf []byte, f *os.File, size int64) ([]byte, error) {
	if int64(cap(buf)-len(buf)) < size {
		grown := make([]byte, len(buf), int(size)+len(buf)+64)
		copy(grown, buf)
		buf = grown
	}
	tmp := make([]byte, 64*1024)
	for {
		n, err := f.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
		}
		if err == io.EOF {
			return buf, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

// runUploaders drains pending/failed tasks with a worker pool.
// Returns nil when the task queue is empty and all workers finished.
func runUploaders(ctx context.Context, cfg *Config, st *Store, prog *Progress) error {
	cli, err := buildClient(cfg)
	if err != nil {
		return err
	}

	pool := &sync.Pool{
		New: func() any {
			b := make([]byte, cfg.BufferSizeMiB*1024*1024)
			return &b
		},
	}

	const batchSize = 512
	var wg sync.WaitGroup
	tasks := make(chan taskKey, batchSize*2)
	var fetchErr atomic.Value

	// periodic result flusher
	flushStop := make(chan struct{})
	go flushLoop(ctx, st, 2*time.Second, flushStop)
	defer close(flushStop)

	// producer: single cursor pass over pending rows (key-ordered)
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		after := ""
		for {
			batch, next, err := fetchTasks(st, cfg, StatePending, batchSize, after, true)
			if err != nil {
				fetchErr.CompareAndSwap(nil, err)
				return
			}
			if len(batch) == 0 {
				break
			}
			after = next
			for _, t := range batch {
				select {
				case tasks <- t:
				case <-ctx.Done():
					return
				}
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	// workers
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range tasks {
				prog.IncInFlight(1)
				uploaded, skipped, err := uploadOne(ctx, cli, cfg, pool, prog, t)
				prog.IncInFlight(-1)
				switch {
				case err != nil && errors.Is(err, context.Canceled):
					// leave as pending
				case err != nil:
					prog.failed.Add(1)
					code, msg := errCode(err)
					logErrf("upload failed: %s: %v\n", t.Key, err)
					st.Record(RecRow{Key: t.Key, State: StateFailed, Code: code, Msg: msg})
				case skipped:
					// already on the cluster (dedup) or gone at source
					prog.skipped.Add(1)
					st.Record(RecRow{Key: t.Key, State: StateDone})
				case uploaded:
					prog.uploaded.Add(1)
					st.Record(RecRow{Key: t.Key, State: StateDone})
				}
			}
		}()
	}

	<-producerDone
	close(tasks)
	wg.Wait()
	st.FlushRecords()

	if err := fetchErr.Load(); err != nil {
		return err.(error)
	}
	return ctx.Err()
}

// flushLoop periodically flushes recorded results so progress is durable
// during long runs and restarts lose at most a few seconds of work.
func flushLoop(ctx context.Context, st *Store, interval time.Duration, stop <-chan struct{}) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			st.FlushRecords()
			return
		case <-stop:
			st.FlushRecords()
			return
		case <-t.C:
			st.FlushRecords()
		}
	}
}

// errCode extracts a short error code for the DB (message capped later).
func errCode(err error) (string, string) {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode(), ae.Error()
	}
	return "error", err.Error()
}

// headExists does a HEAD; returns (exists, size, err).
func headExists(ctx context.Context, cli *s3.Client, bucket, key string) (bool, int64, error) {
	out, err := cli.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var re *smithyhttp.ResponseError
		if errors.As(err, &re) && re.HTTPStatusCode() == http.StatusNotFound {
			return false, 0, nil
		}
		err = wrapNotFound(err)
		var nf *typesNotFound
		if errors.As(err, &nf) {
			return false, 0, nil
		}
		return false, 0, err
	}
	size := int64(0)
	if out.ContentLength != nil {
		size = *out.ContentLength
	}
	return true, size, nil
}

// typesNotFound matches "NotFound" errors that carry no HTTP status in some SDK paths.
type typesNotFound struct{ message string }

func (e *typesNotFound) Error() string { return e.message }

// wrapNotFound converts a raw error into a typesNotFound if it carries a
// NotFound signal (used by servers that omit an HTTP status).
func wrapNotFound(err error) error {
	var ae smithy.APIError
	if errors.As(err, &ae) && (ae.ErrorCode() == "NotFound" || ae.ErrorCode() == "NoSuchKey") {
		return &typesNotFound{message: ae.ErrorCode()}
	}
	return err
}

// ensureBucket creates the bucket if configured and missing.
func ensureBucket(ctx context.Context, cli *s3.Client, cfg *Config) error {
	exists, _, err := headBucket(ctx, cli, cfg.Bucket)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	if !cfg.CreateBucket {
		return fmt.Errorf("bucket %s does not exist on %s (set create_bucket=true to create it)", cfg.Bucket, cfg.Endpoint)
	}
	_, err = cli.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(cfg.Bucket)})
	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) && ae.ErrorCode() == "BucketAlreadyOwnedByYou" {
			return nil
		}
		return err
	}
	logf(os.Stdout, "created bucket %s\n", cfg.Bucket)
	return nil
}

func headBucket(ctx context.Context, cli *s3.Client, bucket string) (bool, int64, error) {
	_, err := cli.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err != nil {
		var re *smithyhttp.ResponseError
		if errors.As(err, &re) && (re.HTTPStatusCode() == http.StatusNotFound || re.HTTPStatusCode() == http.StatusForbidden) {
			return false, 0, nil
		}
		return false, 0, err
	}
	return true, 0, nil
}
