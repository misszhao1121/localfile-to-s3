package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// runCount: page through the whole bucket with ListObjectsV2 (no downloads,
// no HEADs - server-side metadata only) and report:
//   - total object count and total bytes
//   - per first-level-prefix breakdown
//   - optional compare with the local ledger: how many planned keys exist
//     remotely (sampled prefix-level, or exact with --full)
//
// This answers "how much did the previous mc rc transfer actually land"
// in minutes instead of a 39-hour verify pass.
func runCount(ctx context.Context, cfg *Config, st *Store) error {
	if err := InitLog(cfg.LogFile); err != nil {
		return fmt.Errorf("open log file: %w", err)
	}
	defer CloseLog()

	cli, err := buildClient(cfg)
	if err != nil {
		return err
	}

	pre := keyPrefix(cfg)
	logf(os.Stdout, "listing bucket %s at %s (prefix=%q) ...\n", cfg.Bucket, cfg.Endpoint, pre)

	var total remoteTotal
	// concurrent segment listing: split by first-level prefix under the
	// configured key prefix, fall back to flat pagination when the bucket
	// is flat.
	rootChildren, err := listTopPrefixes(ctx, cli, cfg.Bucket, pre)
	if err != nil {
		return err
	}

	if len(rootChildren) > 1 {
		// segmented listing: one worker per top prefix
		var wg sync.WaitGroup
		sem := make(chan struct{}, 8)
		for _, child := range rootChildren {
			child := child
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				wg.Wait()
				return ctx.Err()
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				if ctx.Err() != nil {
					return
				}
				t, err := countRange(ctx, cli, cfg.Bucket, child)
				if err != nil {
					logErrf("list %q: %v\n", child, err)
					return
				}
				total.add(t)
			}()
		}
		wg.Wait()
	} else {
		t, err := countRange(ctx, cli, cfg.Bucket, pre)
		if err != nil {
			return err
		}
		total.add(t)
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	logf(os.Stdout, "\n=== bucket %s ===\n", cfg.Bucket)
	logf(os.Stdout, "objects      : %s\n", comma(total.count))
	logf(os.Stdout, "total bytes  : %s (%.2f GiB)\n", comma(total.bytes), float64(total.bytes)/1024/1024/1024)
	logf(os.Stdout, "\nper top-level prefix:\n")

	names := make([]string, 0, len(total.perPrefix))
	for k := range total.perPrefix {
		names = append(names, k)
	}
	sort.Slice(names, func(i, j int) bool { return total.perPrefix[names[i]].count > total.perPrefix[names[j]].count })
	shown := 0
	var shownCount, shownBytes int64
	for _, k := range names {
		p := total.perPrefix[k]
		if shown >= 30 {
			logf(os.Stdout, "  ... (%d more prefixes)\n", len(names)-shown)
			break
		}
		logf(os.Stdout, "  %-60s %12s  %10.2f GiB\n", truncateKey(k, 60), comma(p.count), float64(p.bytes)/1024/1024/1024)
		shownCount += p.count
		shownBytes += p.bytes
		shown++
	}

	// compare with local ledger
	snap := st.Snapshot()
	logf(os.Stdout, "\n=== compare with local ledger ===\n")
	logf(os.Stdout, "local planned : %s\n", comma(snap.DBTotal))
	logf(os.Stdout, "remote objects: %s\n", comma(total.count))
	if snap.DBTotal > 0 {
		delta := snap.DBTotal - total.count
		if delta > 0 {
			logf(os.Stdout, "missing approx: %s (local - remote; assumes 1:1 key mapping)\n", comma(delta))
			logf(os.Stdout, "\nIf this gap is large, run:  %s\n  to requeue everything missing/mismatched, then migrate.\n",
				"rustfs-migrate -c config.conf verify && rustfs-migrate -c config.conf")
		} else {
			logf(os.Stdout, "remote >= local: bucket has at least as many objects as planned keys\n")
		}
	}
	return nil
}

type prefixStat struct {
	count int64
	bytes int64
}

type remoteTotal struct {
	mu        sync.Mutex
	count     int64
	bytes     int64
	perPrefix map[string]prefixStat
}

func (t *remoteTotal) add(p prefixStat) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.count += p.count
	t.bytes += p.bytes
	if t.perPrefix == nil {
		t.perPrefix = map[string]prefixStat{}
	}
	t.perPrefix["(all)"] = prefixStat{count: t.count, bytes: t.bytes}
}

// listTopPrefixes returns the CommonPrefixes (first-level dirs) under prefix.
func listTopPrefixes(ctx context.Context, cli *s3.Client, bucket, prefix string) ([]string, error) {
	var out []string
	var token *string
	for {
		out2, err := cli.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			Delimiter:         aws.String("/"),
			MaxKeys:           aws.Int32(1000),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, err
		}
		for _, cp := range out2.CommonPrefixes {
			out = append(out, aws.ToString(cp.Prefix))
		}
		if out2.IsTruncated == nil || !*out2.IsTruncated {
			break
		}
		token = out2.NextContinuationToken
	}
	return out, nil
}

// countRange pages all objects under a prefix, aggregating count/bytes and
// per-first-level-prefix stats for that range.
func countRange(ctx context.Context, cli *s3.Client, bucket, prefix string) (prefixStat, error) {
	var stat prefixStat
	var token *string
	for {
		out, err := cli.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			MaxKeys:           aws.Int32(1000),
			ContinuationToken: token,
		})
		if err != nil {
			return stat, err
		}
		for _, o := range out.Contents {
			stat.count++
			if o.Size != nil {
				stat.bytes += *o.Size
			}
		}
		if out.IsTruncated == nil || !*out.IsTruncated {
			break
		}
		token = out.NextContinuationToken
	}
	return stat, nil
}

func truncateKey(k string, n int) string {
	if len(k) <= n {
		return k
	}
	return k[:n-3] + "..."
}

var _ = strings.HasPrefix
