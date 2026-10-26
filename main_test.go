package main

import (
	"bufio"
	"encoding/json/v2"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The test binary runs as the command itself when this is set, so process
// tests exercise the real main without a separate build.
const asCommand = "OMARCHY_SPEEDTEST_RUN_AS_COMMAND"

func TestMain(m *testing.M) {
	if os.Getenv(asCommand) == "1" {
		main()
	}
	os.Exit(m.Run())
}

func command(t *testing.T, url string, args ...string) *exec.Cmd {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), exe, args...)
	cmd.Env = append(os.Environ(), asCommand+"=1", "OMARCHY_SPEEDTEST_URL="+url)
	return cmd
}

func waitFor(t *testing.T, limit time.Duration, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(limit); !cond(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

func TestCommandUsage(t *testing.T) {
	t.Parallel()
	for _, args := range [][]string{nil, {"sideways"}, {"down", "--yaml"}, {"down", "--json", "x"}} {
		out, err := command(t, "http://127.0.0.1:1", args...).CombinedOutput()
		if exit, ok := errors.AsType[*exec.ExitError](err); !ok || exit.ExitCode() != 2 || !strings.Contains(string(out), "Usage:") {
			t.Errorf("%q: err = %v, output %q", args, err, out)
		}
	}
}

func TestCommandRejectsBadEnvironment(t *testing.T) {
	t.Parallel()
	for _, env := range []string{"OMARCHY_SPEEDTEST_CONNECTIONS=0", "OMARCHY_SPEEDTEST_CONNECTIONS=17", "OMARCHY_SPEEDTEST_URL=ftp://x"} {
		cmd := command(t, "http://127.0.0.1:1", "down")
		cmd.Env = append(cmd.Env, env)
		out, err := cmd.CombinedOutput()
		if exit, ok := errors.AsType[*exec.ExitError](err); !ok || exit.ExitCode() != 1 || !strings.Contains(string(out), "Speed test failed") {
			t.Errorf("%s: err = %v, output %q", env, err, out)
		}
	}
}

func TestCommandStreamsProgressThenResult(t *testing.T) {
	t.Parallel()
	_, srv := newService(t, &service{chunkDelay: 15 * time.Millisecond})
	cmd := command(t, srv.URL, "down")
	stdout, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var lines []string
	var firstAt time.Time
	for scanner := bufio.NewScanner(stdout); scanner.Scan(); {
		if firstAt.IsZero() {
			firstAt = time.Now()
		}
		lines = append(lines, scanner.Text())
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	if len(lines) < 3 {
		t.Fatalf("output %q, want progress and a result", lines)
	}
	// The window is five seconds; progress must arrive during it, not at exit.
	if early := time.Since(firstAt); early < 3*time.Second {
		t.Errorf("first update arrived only %v before exit", early.Round(time.Millisecond))
	}
	for _, line := range lines {
		if v, err := strconv.ParseFloat(line, 64); err != nil || v < 0 {
			t.Errorf("line %q is not a rate", line)
		}
	}
}

func TestCommandJSON(t *testing.T) {
	t.Parallel()
	_, srv := newService(t, &service{readDelay: 15 * time.Millisecond})
	cmd := command(t, srv.URL, "up", "--json")
	cmd.Env = append(cmd.Env, "OMARCHY_SPEEDTEST_CONNECTIONS=2")
	out, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	var res result
	if err := json.Unmarshal(out, &res); err != nil {
		t.Fatalf("%q: %v", out, err)
	}
	if res.Bytes <= 0 || res.Seconds < 4 || res.Connections != 2 || res.Mbps <= 0 {
		t.Errorf("result %+v", res)
	}
}

func TestCommandFailsOnServerError(t *testing.T) {
	t.Parallel()
	_, srv := newService(t, &service{truncate: true})
	out, err := command(t, srv.URL, "down").CombinedOutput()
	if exit, ok := errors.AsType[*exec.ExitError](err); !ok || exit.ExitCode() != 1 || !strings.Contains(string(out), "Speed test failed") {
		t.Errorf("err = %v, output %q", err, out)
	}
}

func TestTerminationClosesEveryConnection(t *testing.T) {
	t.Parallel()
	for _, sig := range []syscall.Signal{syscall.SIGTERM, syscall.SIGKILL} {
		t.Run(sig.String(), func(t *testing.T) {
			t.Parallel()
			e, srv := newService(t, &service{readDelay: 15 * time.Millisecond})
			cmd := command(t, srv.URL, "up")
			stdout, _ := cmd.StdoutPipe()
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			if !bufio.NewScanner(stdout).Scan() {
				t.Fatal("no progress before the signal")
			}
			if err := cmd.Process.Signal(sig); err != nil {
				t.Fatal(err)
			}
			if err := cmd.Wait(); err == nil {
				t.Error("a terminated test reported success")
			}
			waitFor(t, 3*time.Second, "every connection to close", func() bool { return e.open.Load() == 0 })
		})
	}
}

func TestStaleParentStopsBeforeNetwork(t *testing.T) {
	t.Parallel()
	e, srv := newService(t, &service{})
	cmd := command(t, srv.URL, "down")
	// The launcher's recorded PID no longer matches: its parent is gone.
	cmd.Env = append(cmd.Env, "OMARCHY_SPEEDTEST_PARENT_PID="+strconv.Itoa(os.Getpid()+1))
	if err := cmd.Run(); err == nil {
		t.Error("ran without its parent")
	}
	if n := e.requests.Load(); n != 0 {
		t.Errorf("made %d requests", n)
	}
}
