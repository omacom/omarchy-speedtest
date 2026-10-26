package main

import (
	"io"
	"net"
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

func TestAnEarlyFinishKeepsTheKernelCount(t *testing.T) {
	t.Parallel()
	// No upload is ever confirmed (each connection closes after it), and
	// slow answers make every worker stop before the window ends. The
	// server's TCP acknowledged the bytes all the same, so they count.
	e, srv := newService(t, &service{closeAfter: true, readDelay: 15 * time.Millisecond, lateAnswer: time.Second, lateAfter: 2500 * time.Millisecond})
	res, err := run(t.Context(), testConfig(srv.URL), upload, nil)
	if err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	served := e.measured
	e.mu.Unlock()
	if res.Bytes < served {
		t.Errorf("counted %d bytes, server read %d in full", res.Bytes, served)
	}
}

func TestAConnectionReplacedBeforeItClosesStillCounts(t *testing.T) {
	// Go's transport can dial a replacement before it closes the old
	// connection. Both are counted, and closing the old one changes nothing.
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { _, _ = io.Copy(io.Discard, conn) }()
		}
	}()
	dial := func() *net.TCPConn {
		var d net.Dialer
		conn, err := d.DialContext(t.Context(), "tcp", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		return conn.(*net.TCPConn)
	}
	send := func(c *countedConn) {
		before, _ := bytesAcked(c.TCPConn)
		if _, err := c.Write(make([]byte, 1<<20)); err != nil {
			t.Fatal(err)
		}
		waitFor(t, 3*time.Second, "the bytes to be acknowledged", func() bool {
			acked, _ := bytesAcked(c.TCPConn)
			return acked-before >= 1<<20
		})
	}

	w := newWorker(testConfig("http://127.0.0.1:1"), upload)
	a := w.track(dial())
	count := newSocketCount([]*worker{w}, false)
	count.start()
	send(a)
	b := w.track(dial())
	send(b)
	n, ok := count.delivered()
	if !ok || n < 2<<20 {
		t.Fatalf("counted %d bytes (ok %v) on two connections that each sent 1 MiB", n, ok)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if after, _ := count.delivered(); after != n {
		t.Errorf("closing the old connection moved the count from %d to %d", n, after)
	}
}
