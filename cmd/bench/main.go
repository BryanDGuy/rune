package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	runesdk "github.com/bryandguy/rune/sdk/go"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var etcdAddr = flag.String("etcd", "localhost:2379", "etcd endpoint")

type benchResult struct {
	setP50        time.Duration
	setP99        time.Duration
	getP50        time.Duration
	getP99        time.Duration
	setThroughput float64 // MB/s
	getThroughput float64 // MB/s
}

var sizes = []struct {
	label string
	bytes int
	iters int
}{
	{"1 MB", 1 << 20, 10},
	{"10 MB", 10 << 20, 10},
	{"100 MB", 100 << 20, 10},
	{"500 MB", 500 << 20, 10},
}

func main() {
	flag.Parse()

	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{*etcdAddr},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "etcd: %v\n", err)
		os.Exit(1)
	}
	defer etcdClient.Close()

	client, err := runesdk.NewClusterClient(etcdClient, "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "cluster client: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	fmt.Printf("Rune cluster benchmark — etcd: %s\n\n", *etcdAddr)

	header := fmt.Sprintf("%-10s  %-10s  %-10s  %-10s  %-10s  %-10s  %-10s",
		"Blob Size", "Set p50", "Set p99", "Get p50", "Get p99", "Set MB/s", "Get MB/s")
	fmt.Println(header)
	fmt.Println(strings.Repeat("-", len(header)))

	for _, sz := range sizes {
		fmt.Fprintf(os.Stderr, "  benching %s (%d iters)...\n", sz.label, sz.iters)
		r, err := bench(client, sz.label, sz.bytes, sz.iters)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  FAILED: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("%-10s  %-10s  %-10s  %-10s  %-10s  %-10.0f  %-10.0f\n",
			sz.label,
			fmtDur(r.setP50), fmtDur(r.setP99),
			fmtDur(r.getP50), fmtDur(r.getP99),
			r.setThroughput, r.getThroughput,
		)
	}
}

func bench(client runesdk.RuneClient, label string, sizeBytes, iters int) (benchResult, error) {
	data := make([]byte, sizeBytes)
	rand.Read(data)
	ctx := context.Background()
	mb := float64(sizeBytes) / (1 << 20)

	// Warmup — one full Set+Get cycle before measuring.
	warmupKey := "bench-warmup-" + label
	if err := client.Set(ctx, warmupKey, bytes.NewReader(data), nil); err != nil {
		return benchResult{}, fmt.Errorf("warmup set: %w", err)
	}
	if err := drain(client, ctx, warmupKey); err != nil {
		return benchResult{}, fmt.Errorf("warmup get: %w", err)
	}

	// Set benchmark — unique key per iteration so there is no server-side reuse.
	setDurs := make([]time.Duration, iters)
	for i := range iters {
		key := fmt.Sprintf("bench-%s-%d", label, i)
		start := time.Now()
		if err := client.Set(ctx, key, bytes.NewReader(data), nil); err != nil {
			return benchResult{}, fmt.Errorf("set iter %d: %w", i, err)
		}
		setDurs[i] = time.Since(start)
	}

	// Get benchmark — read back the same keys written above.
	getDurs := make([]time.Duration, iters)
	for i := range iters {
		key := fmt.Sprintf("bench-%s-%d", label, i)
		start := time.Now()
		if err := drain(client, ctx, key); err != nil {
			return benchResult{}, fmt.Errorf("get iter %d: %w", i, err)
		}
		getDurs[i] = time.Since(start)
	}

	slices.Sort(setDurs)
	slices.Sort(getDurs)

	return benchResult{
		setP50:        pct(setDurs, 50),
		setP99:        pct(setDurs, 99),
		getP50:        pct(getDurs, 50),
		getP99:        pct(getDurs, 99),
		setThroughput: mb / pct(setDurs, 50).Seconds(),
		getThroughput: mb / pct(getDurs, 50).Seconds(),
	}, nil
}

func drain(client runesdk.RuneClient, ctx context.Context, key string) error {
	r, err := client.Get(ctx, key)
	if err != nil {
		return err
	}
	defer r.Close()
	_, err = io.Copy(io.Discard, r)
	return err
}

func pct(sorted []time.Duration, p int) time.Duration {
	idx := (len(sorted) - 1) * p / 100
	return sorted[idx]
}

func fmtDur(d time.Duration) string {
	switch {
	case d < time.Millisecond:
		return fmt.Sprintf("%.1fµs", float64(d.Microseconds()))
	case d < time.Second:
		return fmt.Sprintf("%.1fms", float64(d.Milliseconds()))
	default:
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
}
