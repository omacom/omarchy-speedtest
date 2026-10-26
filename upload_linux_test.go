package main

import (
	"testing"
	"time"
)

func TestALateAnswerDoesNotStretchTheUploadWindow(t *testing.T) {
	t.Parallel()
	// The kernel's count of acknowledged bytes ends the measurement at the
	// window, whatever the last answer's latency.
	e, srv := newService(t, &service{readDelay: 15 * time.Millisecond, lateAnswer: time.Second, lateAfter: 4500 * time.Millisecond})
	res, _ := measure(t, testConfig(srv.URL), upload)
	e.mu.Lock()
	want := mbps(e.rxBytes, e.rxLast.Sub(e.rxFirst))
	e.mu.Unlock()
	if res.Seconds > 5.1 {
		t.Errorf("measured over %.2f s, want the 5 s window", res.Seconds)
	}
	if res.Mbps < 0.9*want || res.Mbps > 1.1*want {
		t.Errorf("rate %.1f Mbps, server received %.1f Mbps", res.Mbps, want)
	}
}

func TestAReplacedConnectionKeepsItsCount(t *testing.T) {
	t.Parallel()
	// A connection that closes mid-window reports what the server
	// acknowledged on it as it closes, so none of its bytes go missing.
	e, srv := newService(t, &service{closeOnce: true, readDelay: 15 * time.Millisecond})
	res, _ := measure(t, testConfig(srv.URL), upload)
	e.mu.Lock()
	served := e.measured
	e.mu.Unlock()
	if res.Bytes < served {
		t.Errorf("counted %d bytes, server read %d in full", res.Bytes, served)
	}
}
