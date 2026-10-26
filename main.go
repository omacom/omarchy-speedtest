// Command omarchy-speedtest measures download or upload speed against
// Omarchy's speed test service.
//
//	omarchy-speedtest down|up [--json]
//
// Without --json it prints the running rate in Mbps about four times a
// second and the final rate last, one number per line. With --json it
// prints one final record.
package main

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
)

func main() {
	if !parentAlive() {
		os.Exit(1)
	}
	// The work is waiting on eight sockets, not computing. More Ps only
	// spread each network wake-up over more threads, which costs CPU without
	// adding throughput.
	if os.Getenv("GOMAXPROCS") == "" {
		runtime.GOMAXPROCS(2)
	}
	os.Exit(cli(os.Args[1:], os.Stdout, os.Stderr))
}

// parentAlive arms parent-death termination before any connection exists,
// then checks the parent is still the one the launcher recorded, which
// closes the race with a parent that died before the signal was armed.
func parentAlive() bool {
	parent := os.Getppid()
	expected := parent
	if pid, err := strconv.Atoi(os.Getenv("OMARCHY_SPEEDTEST_PARENT_PID")); err == nil {
		expected = pid
	}
	if err := armParentDeathSignal(); err != nil {
		return false
	}
	return parent != 1 && os.Getppid() == expected
}

func cli(args []string, stdout, stderr io.Writer) int {
	var dir mode
	var ok bool
	if len(args) > 0 {
		dir, ok = map[string]mode{"down": download, "up": upload}[args[0]]
	}
	asJSON := len(args) == 2 && args[1] == "--json"
	if !ok || len(args) > 2 || len(args) == 2 && !asJSON {
		fmt.Fprintln(stderr, "Usage: omarchy-speedtest down|up [--json]")
		return 2
	}

	cfg, err := configFromEnv()
	if err != nil {
		fmt.Fprintf(stderr, "Speed test failed: %v\n", err)
		return 1
	}
	var progress func(float64)
	if !asJSON {
		progress = func(rate float64) { fmt.Fprintln(stdout, formatMbps(rate)) }
	}
	res, err := run(context.Background(), cfg, dir, progress)
	if err != nil {
		fmt.Fprintf(stderr, "Speed test failed: %v\n", err)
		return 1
	}
	if asJSON {
		out, err := json.Marshal(res)
		if err != nil {
			fmt.Fprintf(stderr, "Speed test failed: %v\n", err)
			return 1
		}
		fmt.Fprintf(stdout, "%s\n", out)
	} else {
		fmt.Fprintln(stdout, formatMbps(res.Mbps))
	}
	return 0
}

func configFromEnv() (config, error) {
	cfg := defaultConfig()
	if value, ok := os.LookupEnv("OMARCHY_SPEEDTEST_CONNECTIONS"); ok {
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 || n > maxConnections {
			return cfg, fmt.Errorf("connection count must be 1..%d", maxConnections)
		}
		cfg.connections = n
	}
	if value, ok := os.LookupEnv("OMARCHY_SPEEDTEST_URL"); ok {
		u, err := url.Parse(value)
		if err != nil || u.Scheme != "https" && u.Scheme != "http" || u.Host == "" {
			return cfg, errors.New("invalid speed test URL")
		}
		cfg.url = strings.TrimRight(value, "/")
	}
	return cfg, nil
}

// formatMbps keeps one decimal below 10 Mbps, where it still means
// something, and whole numbers above.
func formatMbps(rate float64) string {
	if rate < 10 {
		return strconv.FormatFloat(max(rate, 0), 'f', 1, 64)
	}
	return strconv.FormatFloat(rate, 'f', 0, 64)
}
