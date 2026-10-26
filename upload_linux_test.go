package main

import (
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unsafe"
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

// bytesReceived is tcpi_bytes_received, at byte 128 of struct tcp_info.
func bytesReceived(t *testing.T, conn net.Conn) uint64 {
	t.Helper()
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		return 0
	}
	raw, err := tcp.SyscallConn()
	if err != nil {
		return 0
	}
	var n uint64
	_ = raw.Control(func(fd uintptr) {
		var info [256]byte
		size := uint32(len(info))
		_, _, errno := syscall.Syscall6(syscall.SYS_GETSOCKOPT, fd, syscall.IPPROTO_TCP, syscall.TCP_INFO,
			uintptr(unsafe.Pointer(&info[0])), uintptr(unsafe.Pointer(&size)), 0)
		if errno == 0 && size >= 136 {
			n = binary.NativeEndian.Uint64(info[128:136])
		}
	})
	return n
}

// received records what the service's TCP has received over time, so a test
// can read it afterwards over the client's own window.
type received struct {
	mu   sync.Mutex
	at   []time.Time
	n    []uint64
	stop chan struct{}
	done chan struct{}
}

func recordReceived(t *testing.T, e *service) *received {
	t.Helper()
	r := &received{stop: make(chan struct{}), done: make(chan struct{})}
	total := func() (n uint64) {
		e.mu.Lock()
		defer e.mu.Unlock()
		for _, c := range e.serverConns {
			n += bytesReceived(t, c)
		}
		return n
	}
	go func() {
		defer close(r.done)
		for {
			n := total()
			r.mu.Lock()
			r.at, r.n = append(r.at, time.Now()), append(r.n, n)
			r.mu.Unlock()
			select {
			case <-r.stop:
				return
			case <-time.After(100 * time.Microsecond):
			}
		}
	}()
	t.Cleanup(r.finish)
	return r
}

func (r *received) finish() {
	select {
	case <-r.stop:
	default:
		close(r.stop)
	}
	<-r.done
}

// between is what was received from a to b, read between samples.
func (r *received) between(a, b time.Time) float64 {
	r.finish()
	at := func(when time.Time) float64 {
		i, _ := slices.BinarySearchFunc(r.at, when, func(t, w time.Time) int { return t.Compare(w) })
		i = min(max(i, 1), len(r.at)-1)
		span := r.at[i].Sub(r.at[i-1]).Seconds()
		f := 0.0
		if span > 0 {
			f = min(max(when.Sub(r.at[i-1]).Seconds()/span, 0), 1)
		}
		return float64(r.n[i-1]) + f*(float64(r.n[i])-float64(r.n[i-1]))
	}
	return at(b) - at(a)
}

// window notes when the client's measurement window opens and when its
// upload count is read.
type window struct{ opened, read atomic.Int64 }

// watchWindow also runs hold, if any, after the count is read and before
// run gets it.
func watchWindow(cfg *config, hold func()) *window {
	w := &window{}
	cfg.test = &testHooks{
		opened: func(t time.Time) { w.opened.Store(t.UnixNano()) },
		read: func(t time.Time) {
			w.read.Store(t.UnixNano())
			if hold != nil {
				hold()
			}
		},
	}
	return w
}

func (w *window) start() time.Time { return time.Unix(0, w.opened.Load()) }

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
	// gets round to it: it matches what the server's TCP received in the
	// window.
	e, srv := newService(t, &service{readDelay: 15 * time.Millisecond, slowAfter: 4500 * time.Millisecond, slowReadDelay: 400 * time.Millisecond, rcvBuf: 64 << 10})
	rx := recordReceived(t, e)
	// The first update comes 250 ms into the window; block on the 4.75 s one.
	progress, blocked := blockOnce(4400*time.Millisecond, 3*time.Second)
	cfg := testConfig(srv.URL)
	win := watchWindow(&cfg, nil)
	res, err := run(t.Context(), cfg, upload, progress)
	if err != nil {
		t.Fatal(err)
	}
	win.readOnTime(t, cfg.window)
	inWindow := rx.between(win.start(), win.start().Add(cfg.window))
	if !blocked.Load() {
		t.Fatal("progress never blocked")
	}
	if float64(res.Bytes) > 1.01*inWindow || float64(res.Bytes) < 0.99*inWindow {
		t.Errorf("counted %d bytes, the server's TCP received %.0f in the window", res.Bytes, inWindow)
	}
}

func TestAWorkerThatEndsAfterTheWindowKeepsTheWindowsCount(t *testing.T) {
	t.Parallel()
	// One connection whose last upload is answered after the window closes,
	// and whose end reaches run before the window's count does: the count
	// is held back until the worker has finished and closed its connection.
	// The worker's end must not reopen the count.
	e, srv := newService(t, &service{readDelay: 15 * time.Millisecond, lateAnswer: time.Second, lateAfter: 4500 * time.Millisecond})
	rx := recordReceived(t, e)
	cfg := testConfig(srv.URL)
	cfg.connections = 1
	win := watchWindow(&cfg, func() {
		// On run's timer goroutine, so no t.Fatal here.
		for deadline := time.Now().Add(3 * time.Second); e.open.Load() != 0; time.Sleep(10 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Error("the worker never finished")
				return
			}
		}
	})
	res, err := run(t.Context(), cfg, upload, nil)
	if err != nil {
		t.Fatal(err)
	}
	win.readOnTime(t, cfg.window)
	inWindow := rx.between(win.start(), win.start().Add(cfg.window))
	if res.Seconds != cfg.window.Seconds() {
		t.Errorf("divided by %.2f s, the window is %.0f s", res.Seconds, cfg.window.Seconds())
	}
	if float64(res.Bytes) > 1.01*inWindow || float64(res.Bytes) < 0.99*inWindow {
		t.Errorf("counted %d bytes, the server's TCP received %.0f in the window", res.Bytes, inWindow)
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
