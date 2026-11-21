package main

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"
	"syscall"
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

func TestAConnectionThatNeverConnectsDoesNotTurnOffTheKernelCount(t *testing.T) {
	t.Parallel()
	// The left-out worker must not reach the kernel's upload count: it has
	// no connection, which turns that count off for every worker, and the
	// result would fall back to confirmed requests. The kernel counts the
	// request lines on top of the payload, so its count is higher than the
	// payload the server read; the confirmed count is exactly that payload.
	e, srv := newService(t, &service{readDelay: 15 * time.Millisecond})
	cfg := testConfig(srv.URL)
	var dials atomic.Int64
	cfg.test = &testHooks{dialControl: func(string, string, syscall.RawConn) error {
		if dials.Add(1) == 1 {
			time.Sleep(cfg.warmup + time.Second)
		}
		return nil
	}}
	res, _ := measure(t, cfg, upload)
	if res.Connections != cfg.connections-1 {
		t.Errorf("measured on %d connections, want %d", res.Connections, cfg.connections-1)
	}
	e.mu.Lock()
	served := e.measured
	e.mu.Unlock()
	if served == 0 {
		t.Fatal("the server saw no measured payload")
	}
	if res.Bytes <= served {
		t.Errorf("counted %d bytes, want more than the %d payload bytes the server read", res.Bytes, served)
	}
}

func TestUploadsThroughAProxyCountConfirmedRequests(t *testing.T) {
	t.Parallel()
	// Through a proxy the kernel would count what the proxy acknowledged,
	// which may be buffered there, so uploads count confirmed requests: what
	// the server itself read in full.
	e, srv := newService(t, &service{readDelay: 15 * time.Millisecond})
	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httptest.NewServer(httputil.NewSingleHostReverseProxy(target))
	t.Cleanup(proxy.Close)
	proxyURL, err := url.Parse(proxy.URL)
	if err != nil {
		t.Fatal(err)
	}
	cfg := testConfig(srv.URL)
	cfg.proxy = http.ProxyURL(proxyURL)
	res, _ := measure(t, cfg, upload)
	e.mu.Lock()
	served := e.measured
	e.mu.Unlock()
	if res.Bytes != served {
		t.Errorf("counted %d bytes, server read %d in full", res.Bytes, served)
	}
}

// window notes when the client's measurement window opens, when its
// upload count is read and what that count was.
type window struct {
	opened, read atomic.Int64
	counted      atomic.Int64
}

// watchWindow also runs hold, if any, after the count is read and before
// run gets it.
func watchWindow(cfg *config, hold func()) *window {
	w := &window{}
	cfg.test = &testHooks{
		opened: func(t time.Time) { w.opened.Store(t.UnixNano()) },
		read: func(t time.Time, n int64) {
			w.read.Store(t.UnixNano())
			w.counted.Store(n)
			if hold != nil {
				hold()
			}
		},
	}
	return w
}

// count is the upload count read as the window closed.
func (w *window) count() int64 { return w.counted.Load() }

// readOnTime fails unless the count was read as the window closed.
func (w *window) readOnTime(t *testing.T, length time.Duration) {
	t.Helper()
	if late := time.Duration(w.read.Load()-w.opened.Load()) - length; late < 0 || late > 100*time.Millisecond {
		t.Errorf("count read %v after the window closed", late)
	}
}

// blockOnce returns a progress callback that stops reading for hold, once,
// after its first update plus after.
func blockOnce(after, hold time.Duration) (func(float64), *atomic.Bool) {
	var blocked atomic.Bool
	var first time.Time
	return func(float64) {
		if first.IsZero() {
			first = time.Now()
		}
		if time.Since(first) >= after && blocked.CompareAndSwap(false, true) {
			time.Sleep(hold)
		}
	}, &blocked
}

func TestABlockedProgressCannotAddBytesAfterTheWindow(t *testing.T) {
	t.Parallel()
	// The panel stops reading for three seconds just before the window
	// closes, while uploads are still in flight because the link slowed.
	// The count is taken when the window closes, not when the main loop
	// gets round to it: the result is the count read at the close.
	_, srv := newService(t, &service{readDelay: 15 * time.Millisecond, slowAfter: 4500 * time.Millisecond, slowReadDelay: 400 * time.Millisecond, rcvBuf: 64 << 10})
	// The first update comes 250 ms into the window; block on the 4.75 s one.
	progress, blocked := blockOnce(4400*time.Millisecond, 3*time.Second)
	cfg := testConfig(srv.URL)
	win := watchWindow(&cfg, nil)
	res, err := run(t.Context(), cfg, upload, progress)
	if err != nil {
		t.Fatal(err)
	}
	win.readOnTime(t, cfg.window)
	if !blocked.Load() {
		t.Fatal("progress never blocked")
	}
	if win.count() == 0 {
		t.Fatal("no upload count was read as the window closed")
	}
	if res.Bytes != win.count() {
		t.Errorf("counted %d bytes, the window closed on %d", res.Bytes, win.count())
	}
}

func TestAWorkerThatEndsAfterTheWindowKeepsTheWindowsCount(t *testing.T) {
	t.Parallel()
	// One connection whose last upload is answered after the window closes,
	// and whose end reaches run before the window's count does: the count
	// is held back until the worker has finished and closed its connection.
	// The worker's end must not reopen the count.
	e, srv := newService(t, &service{readDelay: 15 * time.Millisecond, lateAnswer: time.Second, lateAfter: 4500 * time.Millisecond})
	cfg := testConfig(srv.URL)
	cfg.connections = 1
	var workerEnded atomic.Bool
	win := watchWindow(&cfg, func() {
		// On run's timer goroutine, so no t.Fatal here.
		for deadline := time.Now().Add(3 * time.Second); e.open.Load() != 0; time.Sleep(10 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Error("the worker never finished")
				return
			}
		}
		workerEnded.Store(true)
	})
	res, err := run(t.Context(), cfg, upload, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !workerEnded.Load() {
		t.Fatal("the count reached run before the worker had ended")
	}
	if res.Seconds != cfg.window.Seconds() {
		t.Errorf("divided by %.2f s, the window is %.0f s", res.Seconds, cfg.window.Seconds())
	}
	if win.count() == 0 {
		t.Fatal("no upload count was read as the window closed")
	}
	if res.Bytes != win.count() {
		t.Errorf("counted %d bytes, the window closed on %d", res.Bytes, win.count())
	}
}

func TestAFailureBeforeTheWindowClosesStillFails(t *testing.T) {
	t.Parallel()
	// The service misreports the first upload, and the panel does not read
	// again until after the window has closed. The failure stands.
	_, srv := newService(t, &service{badAck: true, badAckAfter: 500 * time.Millisecond})
	progress, _ := blockOnce(0, 6*time.Second)
	_, err := run(t.Context(), testConfig(srv.URL), upload, progress)
	if err == nil || !strings.Contains(err.Error(), "acknowledge") {
		t.Fatalf("got %v, want the acknowledgement error", err)
	}
}
