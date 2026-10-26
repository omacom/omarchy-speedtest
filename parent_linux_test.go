package main

import (
	"bufio"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestParentDeathStopsTransfers(t *testing.T) {
	t.Parallel()
	e, srv := newService(t, &service{chunkDelay: 15 * time.Millisecond})
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	// A shell stands in for the launcher: it starts the test and waits. The
	// test writes to /dev/null, so a closed pipe cannot be what stops it.
	parent := exec.CommandContext(t.Context(), "sh", "-c", `"$0" down >/dev/null & echo $!; wait`, exe)
	parent.Env = append(os.Environ(), asCommand+"=1", "OMARCHY_SPEEDTEST_URL="+srv.URL)
	stdout, _ := parent.StdoutPipe()
	if err := parent.Start(); err != nil {
		t.Fatal(err)
	}
	// Cleanup after a failure: the parent may already be gone.
	t.Cleanup(func() { _ = parent.Process.Kill(); _ = parent.Wait() })
	lines := bufio.NewScanner(stdout)
	if !lines.Scan() {
		t.Fatal("no PID from the parent")
	}
	child, err := strconv.Atoi(lines.Text())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Kill(child, syscall.SIGKILL) })
	waitFor(t, 15*time.Second, "the measurement to start", func() bool {
		e.mu.Lock()
		defer e.mu.Unlock()
		return e.measured > 0
	})

	if err := parent.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = parent.Wait() // reports the kill above
	waitFor(t, 3*time.Second, "the orphaned test to exit", func() bool {
		stat, err := os.ReadFile("/proc/" + strconv.Itoa(child) + "/stat")
		return err != nil || strings.Contains(string(stat), ") Z ")
	})
	waitFor(t, 3*time.Second, "every connection to close", func() bool { return e.open.Load() == 0 })
}
