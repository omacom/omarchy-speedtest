package main

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// service is a stand-in for the speed test service. It records what it
// served in the measured phase so a test can compare the client's result
// with the server's own count.
type service struct {
	latency     time.Duration // before every response
	chunk       int           // download write size
	chunkDelay  time.Duration // after every download write
	readDelay   time.Duration // after every 64 KiB of upload read
	upRate      float64       // upload bytes per second per connection, if set
	truncate    bool          // send half of every measured download
	badAck      bool          // acknowledge one byte less than a measured upload
	badAckAfter time.Duration // from this long into the measurement
	closeAfter  bool          // close the connection after measured uploads
	closeOnce   bool          // close one connection after its third measured upload
	shortBody   bool          // honestly send half of every measured download
	earlyAck    bool          // acknowledge measured uploads before reading them, then close
	// Hold back the first byte of every measured download, like a slow first
	// byte or a TCP ramp at the start of the window.
	firstByteDelay time.Duration
	// One bottleneck for every measured download, in bytes per second, and
	// a first byte held back a second on every other connection.
	sharedRate float64
	// The listener's receive buffer, so the server's kernel acknowledges
	// uploads only as fast as the handler reads them.
	rcvBuf int
	// One slow link under every request, warm-up included: body bytes in
	// either direction, in bytes per second shared by all connections.
	linkRate         float64
	staggerFirstByte bool
	// Answer measured uploads that arrive lateAfter or later into the
	// measurement only after lateAnswer: the slow last answer that used to
	// stretch the upload clock.
	lateAnswer, lateAfter time.Duration

	mu          sync.Mutex
	measured    int64     // payload bytes in the measured phase
	first, last time.Time // first measured request, last measured byte
	firstWrite  time.Time // first measured byte written
	// Measured upload bytes as they arrive, before any answer.
	rxBytes         int64
	rxFirst, rxLast time.Time
	rxTimes         []time.Time // arrival of each measured upload chunk
	rxSizes         []int64
	// Read slower from slowAfter into the measurement on, so uploads are
	// still in flight when the window closes.
	slowAfter     time.Duration
	slowReadDelay time.Duration
	serverConns   []net.Conn // every connection the service accepted
	uploads       []int64    // measured upload sizes
	perConn       map[string]int
	dropped       int64 // the upload whose connection closeOnce closed

	closed   atomic.Bool
	paceNext time.Time      // when the shared bottleneck is next free
	linkNext time.Time      // when the slow link is next free
	seen     map[string]int // measured download requests per connection
	requests atomic.Int64
	http2    atomic.Int64 // requests that did not use HTTP/1.1
	noise    atomic.Int64 // bytes served to unrelated traffic
	open     atomic.Int64 // open connections
}

func newService(t *testing.T, e *service) (*service, *httptest.Server) {
	t.Helper()
	if e.chunk == 0 {
		e.chunk = 64 << 10
	}
	srv := httptest.NewUnstartedServer(e)
	if e.rcvBuf > 0 {
		lc := net.ListenConfig{Control: func(_, _ string, c syscall.RawConn) error {
			var err error
			if cerr := c.Control(func(fd uintptr) {
				err = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, e.rcvBuf)
			}); cerr != nil {
				return cerr
			}
			return err
		}}
		ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		_ = srv.Listener.Close()
		srv.Listener = ln
	}
	srv.Config.ConnState = func(c net.Conn, state http.ConnState) {
		switch state {
		case http.StateNew:
			e.open.Add(1)
			e.mu.Lock()
			e.serverConns = append(e.serverConns, c)
			e.mu.Unlock()
		case http.StateClosed, http.StateHijacked:
			e.open.Add(-1)
		case http.StateActive, http.StateIdle:
			// The connection is still open.
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)
	return e, srv
}

func (e *service) mark(n int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := time.Now()
	if e.first.IsZero() {
		e.first = now
	}
	if n > 0 {
		e.measured += n
		e.last = now
		if e.firstWrite.IsZero() {
			e.firstWrite = now
		}
	}
}

func (e *service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	measured := r.Header.Get("X-Speedtest-Phase") == "measure"
	if r.URL.Path != "/noise" {
		e.requests.Add(1)
	}
	if r.ProtoMajor != 1 {
		e.http2.Add(1)
	}
	time.Sleep(e.latency)
	switch r.URL.Path {
	case "/latency":
		w.WriteHeader(http.StatusNoContent)
	case "/noise":
		e.serve(w, 8<<20, 64<<10, 6*time.Millisecond, func(n int64) { e.noise.Add(n) })
	case "/down":
		size, err := strconv.ParseInt(r.URL.Query().Get("bytes"), 10, 64)
		if err != nil {
			http.Error(w, "bad size", http.StatusBadRequest)
			return
		}
		if measured {
			e.mark(0)
			delay := e.firstByteDelay
			if e.staggerFirstByte {
				e.mu.Lock()
				if e.seen == nil {
					e.seen = map[string]int{}
				}
				if e.seen[r.RemoteAddr] == 0 && len(e.seen)%2 == 1 {
					delay = time.Second
				}
				e.seen[r.RemoteAddr]++
				e.mu.Unlock()
			}
			time.Sleep(delay)
		}
		truncate, short := e.truncate && measured, e.shortBody && measured
		limit := size
		if truncate || short {
			limit = size / 2
		}
		if short {
			size = limit
		}
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		e.serve(w, limit, e.chunk, e.chunkDelay, func(n int64) {
			e.link(n)
			if measured {
				e.pace(n)
				e.mark(n)
			}
		})
		if truncate {
			panic(http.ErrAbortHandler)
		}
	case "/upload":
		late := false
		if measured {
			e.mark(0)
			e.mu.Lock()
			e.uploads = append(e.uploads, r.ContentLength)
			late = e.lateAnswer > 0 && time.Since(e.first) >= e.lateAfter
			e.mu.Unlock()
		}
		if measured && e.earlyAck {
			// Acknowledge before reading the body, then read it.
			_ = http.NewResponseController(w).EnableFullDuplex()
			w.Header().Set("Connection", "close")
			_, _ = w.Write([]byte(`{"acceptedBytes":` + strconv.FormatInt(r.ContentLength, 10) + `}`))
			_ = http.NewResponseController(w).Flush()
			_, _ = io.Copy(io.Discard, r.Body)
			return
		}
		chunk := 64 << 10
		if e.upRate > 0 {
			chunk = 4 << 10
		}
		buf := make([]byte, chunk)
		var got int64
		for {
			n, err := io.ReadFull(r.Body, buf)
			got += int64(n)
			if measured && n > 0 {
				e.mu.Lock()
				if e.rxFirst.IsZero() {
					e.rxFirst = time.Now()
				}
				e.rxBytes += int64(n)
				e.rxLast = time.Now()
				e.rxTimes = append(e.rxTimes, e.rxLast)
				e.rxSizes = append(e.rxSizes, int64(n))
				e.mu.Unlock()
			}
			if e.upRate > 0 {
				time.Sleep(time.Duration(float64(n) / e.upRate * float64(time.Second)))
			}
			e.link(int64(n))
			if err != nil {
				break
			}
			delay := e.readDelay
			if e.slowAfter > 0 {
				e.mu.Lock()
				if !e.first.IsZero() && time.Since(e.first) > e.slowAfter {
					delay = e.slowReadDelay
				}
				e.mu.Unlock()
			}
			time.Sleep(delay)
		}
		if got != r.ContentLength {
			return
		}
		if measured {
			e.mark(got)
		}
		if e.badAck && measured {
			e.mu.Lock()
			if time.Since(e.first) >= e.badAckAfter {
				got--
			}
			e.mu.Unlock()
		}
		replace := false
		if measured && e.closeOnce {
			e.mu.Lock()
			if e.perConn == nil {
				e.perConn = map[string]int{}
			}
			e.perConn[r.RemoteAddr]++
			if e.perConn[r.RemoteAddr] == 3 && e.closed.CompareAndSwap(false, true) {
				replace, e.dropped = true, got
			}
			e.mu.Unlock()
		}
		if measured && (e.closeAfter || replace) {
			w.Header().Set("Connection", "close")
		}
		if late {
			time.Sleep(e.lateAnswer)
		}
		_, _ = w.Write([]byte(`{"acceptedBytes":` + strconv.FormatInt(got, 10) + `}`))
	default:
		http.NotFound(w, r)
	}
}

// pace holds a write until the shared bottleneck has room for it.
func (e *service) pace(n int64) { e.hold(e.sharedRate, &e.paceNext, n) }

// link holds n bytes on the slow link, if there is one.
func (e *service) link(n int64) { e.hold(e.linkRate, &e.linkNext, n) }

// hold delays n bytes so the bytes it holds pass at rate bytes per second,
// shared by every caller that holds on the same next.
func (e *service) hold(rate float64, next *time.Time, n int64) {
	if rate == 0 {
		return
	}
	e.mu.Lock()
	now := time.Now()
	if next.Before(now) {
		*next = now
	}
	at := *next
	*next = next.Add(time.Duration(float64(n) / rate * float64(time.Second)))
	e.mu.Unlock()
	time.Sleep(time.Until(at))
}

func (e *service) serve(w http.ResponseWriter, size int64, chunk int, delay time.Duration, sent func(int64)) {
	buf := make([]byte, chunk)
	rc := http.NewResponseController(w)
	for left := size; left > 0; {
		n := int(min(int64(chunk), left))
		sent(int64(n))
		if _, err := w.Write(buf[:n]); err != nil {
			return
		}
		if rc.Flush() != nil {
			return
		}
		left -= int64(n)
		time.Sleep(delay)
	}
}

func testConfig(url string) config {
	cfg := defaultConfig()
	cfg.url = url
	return cfg
}

func measure(t *testing.T, cfg config, dir mode) (result, []float64) {
	t.Helper()
	var mu sync.Mutex
	var steps []float64
	res, err := run(t.Context(), cfg, dir, func(rate float64) {
		mu.Lock()
		steps = append(steps, rate)
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	return res, steps
}

func TestCountsOnlyTestPayloads(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		dir           mode
		confirmedOnly bool
	}{{"down", download, false}, {"up", upload, false}, {"up/confirmed", upload, true}} {
		dir := tc.dir
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e, srv := newService(t, &service{chunkDelay: 15 * time.Millisecond, readDelay: 15 * time.Millisecond})

			// Unrelated traffic on the same link must not reach the result.
			ctx, stop := context.WithCancel(t.Context())
			var wg sync.WaitGroup
			wg.Go(func() {
				for ctx.Err() == nil {
					req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/noise", nil)
					resp, err := http.DefaultClient.Do(req)
					if err != nil {
						return
					}
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
				}
			})
			cfg := testConfig(srv.URL)
			cfg.test = &testHooks{confirmedOnly: tc.confirmedOnly}
			res, steps := measure(t, cfg, dir)
			stop()
			wg.Wait()

			e.mu.Lock()
			served, elapsed := e.measured, e.last.Sub(e.first)
			e.mu.Unlock()
			want := mbps(served, elapsed)
			if res.Mbps < 0.8*want || res.Mbps > 1.2*want {
				t.Errorf("rate %.1f Mbps, server saw %.1f Mbps", res.Mbps, want)
			}
			// Confirmed requests are exactly what the server read in full. The
			// kernel also counts request lines and the bytes it acknowledged
			// before the window closed, so never less.
			if tc.confirmedOnly && res.Bytes != served {
				t.Errorf("counted %d upload bytes, server acknowledged %d", res.Bytes, served)
			}
			if dir == upload && res.Bytes < served {
				t.Errorf("counted %d upload bytes, server read %d in full", res.Bytes, served)
			}
			if dir == download && res.Bytes > served {
				t.Errorf("counted %d download bytes, server sent only %d", res.Bytes, served)
			}
			if noise := e.noise.Load(); noise < served/4 {
				t.Errorf("background traffic too small to prove anything: %d of %d", noise, served)
			}
			if len(steps) < 2 {
				t.Errorf("got %d progress updates, want several", len(steps))
			}
			if res.Connections != defaultConnections {
				t.Errorf("connections = %d", res.Connections)
			}
		})
	}
}

func TestASlowFirstByteDoesNotShortenTheDownloadWindow(t *testing.T) {
	t.Parallel()
	// The window opens at the first measured byte, so a second without
	// bytes at the start is not counted against the link.
	e, srv := newService(t, &service{chunkDelay: 15 * time.Millisecond, firstByteDelay: time.Second})
	res, _ := measure(t, testConfig(srv.URL), download)
	e.mu.Lock()
	want := mbps(e.measured, e.last.Sub(e.firstWrite))
	e.mu.Unlock()
	if res.Mbps < 0.95*want || res.Mbps > 1.05*want {
		t.Errorf("rate %.1f Mbps, server sent %.1f Mbps once it started", res.Mbps, want)
	}
}

func TestAFirstByteLaterThanTheDrainIsReported(t *testing.T) {
	t.Parallel()
	// The window opens at the first byte, but every transfer must end
	// within the drain allowance of the scheduled window. A service that
	// waits longer than that cannot be measured, and the error says why.
	_, srv := newService(t, &service{firstByteDelay: 3500 * time.Millisecond})
	_, err := run(t.Context(), testConfig(srv.URL), download, nil)
	if err == nil || !strings.Contains(err.Error(), "first byte") {
		t.Fatalf("got %v, want an error about the first byte", err)
	}
}

func TestStaggeredConnectionsCannotExceedTheLink(t *testing.T) {
	t.Parallel()
	// Eight connections share one 240 Mbit/s bottleneck, and half of them
	// see their first byte a second late. Bytes over a window must never
	// exceed what the bottleneck carries in that window.
	const rate = 30e6
	_, srv := newService(t, &service{sharedRate: rate, staggerFirstByte: true})
	res, _ := measure(t, testConfig(srv.URL), download)
	if limit := rate * 8 / 1e6; res.Mbps > 1.05*limit || res.Mbps < 0.85*limit {
		t.Errorf("rate %.1f Mbps through a %.0f Mbit/s bottleneck", res.Mbps, limit)
	}
}

func TestSlowHealthyDownloadsFinish(t *testing.T) {
	t.Parallel()
	_, srv := newService(t, &service{chunkDelay: 240 * time.Millisecond})
	if res, _ := measure(t, testConfig(srv.URL), download); res.Bytes == 0 {
		t.Error("no bytes measured")
	}
}

func TestVerySlowLinksStillMeasure(t *testing.T) {
	t.Parallel()
	// About 14 kB/s per connection: one 64 KiB warm-up takes 4.5 s.
	_, srv := newService(t, &service{chunk: 1024, chunkDelay: 70 * time.Millisecond})
	if res, _ := measure(t, testConfig(srv.URL), download); res.Bytes == 0 {
		t.Error("no bytes measured")
	}
}

func TestASlowSharedLinkStillMeasures(t *testing.T) {
	t.Parallel()
	// 0.4 Mbit/s shared by all eight connections, warm-up included: eight
	// 64 KiB warm-up downloads alone take 10 s, longer than warm-up's budget.
	// A slow link is a result, not an error.
	const rate = 50e3
	_, srv := newService(t, &service{chunk: 4 << 10, linkRate: rate})
	res, err := run(t.Context(), testConfig(srv.URL), download, nil)
	if err != nil {
		t.Fatal(err)
	}
	if limit := rate * 8 / 1e6; res.Mbps > 1.05*limit || res.Mbps < 0.7*limit {
		t.Errorf("rate %.3f Mbps through a %.1f Mbit/s link", res.Mbps, limit)
	}
}

func TestVerySlowUplinksStillMeasure(t *testing.T) {
	t.Parallel()
	// 512 kbit/s shared by eight connections: 8 kB/s each, so a 64 KiB
	// request would take 8 s, longer than warm-up and the window.
	_, srv := newService(t, &service{upRate: 8000})
	if res, _ := measure(t, testConfig(srv.URL), upload); res.Bytes == 0 {
		t.Error("no bytes measured")
	}
}

func TestConfirmedUploadsStopWhenAnswersTurnSlow(t *testing.T) {
	t.Parallel()
	// From 2.5 s into the window every answer comes a second late. Without
	// the kernel's count the last answer ends the clock, so the client must
	// stop starting uploads it cannot have answered inside the window. The
	// rate estimate, which absorbs the late answers, is what stops it.
	_, srv := newService(t, &service{readDelay: 15 * time.Millisecond, lateAnswer: time.Second, lateAfter: 2500 * time.Millisecond})
	cfg := testConfig(srv.URL)
	cfg.test = &testHooks{confirmedOnly: true}
	res, _ := measure(t, cfg, upload)
	if res.Seconds > 5.3 {
		t.Errorf("measured over %.2f s, want the 5 s window and one confirmation", res.Seconds)
	}
}

func TestLatencyDoesNotPinUploadsToSmallRequests(t *testing.T) {
	t.Parallel()
	e, srv := newService(t, &service{latency: 50 * time.Millisecond})
	measure(t, testConfig(srv.URL), upload)
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.uploads) == 0 || e.uploads[len(e.uploads)/2] < 1<<20 {
		t.Errorf("measured upload sizes %v, want most at least 1 MiB", e.uploads)
	}
}

func TestRejects(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		svc           *service
		dir           mode
		want          string
		confirmedOnly bool
	}{
		{"short acknowledgement", &service{badAck: true}, upload, "acknowledge", false},
		{"truncated download", &service{truncate: true}, download, "EOF", false},
		{"uploads never confirmed on their connection", &service{closeAfter: true}, upload, "confirm", true},
		// Acknowledgements that arrive before the body make request times
		// meaningless, so which check stops the test varies: any error will do,
		// a result will not.
		{"early acknowledgement on a closing connection", &service{earlyAck: true}, upload, "", true},
		// 3.2 kB/s per connection: warm-up finishes, but no 16 KiB upload fits
		// the window. A working link must not read as zero.
		{"uplink too slow for the window", &service{upRate: 3200}, upload, "too slow", false},
		{"download shorter than requested", &service{shortBody: true}, download, "incomplete", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, srv := newService(t, tc.svc)
			cfg := testConfig(srv.URL)
			cfg.test = &testHooks{confirmedOnly: tc.confirmedOnly}
			_, err := run(t.Context(), cfg, tc.dir, nil)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

func TestConfirmedCountDropsOnlyAReplacedConnectionsUnconfirmedUpload(t *testing.T) {
	t.Parallel()
	// The replaced connection has already had two uploads confirmed; only
	// the one its replacement leaves unconfirmed may go uncounted.
	e, srv := newService(t, &service{closeOnce: true, readDelay: 15 * time.Millisecond})
	cfg := testConfig(srv.URL)
	cfg.test = &testHooks{confirmedOnly: true}
	res, _ := measure(t, cfg, upload)
	e.mu.Lock()
	served, dropped := e.measured, e.dropped
	e.mu.Unlock()
	if dropped == 0 {
		t.Fatal("no connection reached its third upload")
	}
	if res.Bytes != served-dropped {
		t.Errorf("counted %d bytes, want %d received minus %d unconfirmed", res.Bytes, served, dropped)
	}
}

func TestRunLeavesNoConnectionsOpen(t *testing.T) {
	t.Parallel()
	for _, dir := range []mode{download, upload} {
		e, srv := newService(t, &service{chunkDelay: 15 * time.Millisecond, readDelay: 15 * time.Millisecond})
		measure(t, testConfig(srv.URL), dir)
		waitFor(t, time.Second, "every connection to close", func() bool { return e.open.Load() == 0 })
	}
}

// slowHandshakes holds each connection's first write, the server's TLS
// handshake flight, for delay.
type slowHandshakes struct {
	net.Listener
	delay time.Duration
}

func (l *slowHandshakes) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &slowHandshakeConn{Conn: c, delay: l.delay}, nil
}

type slowHandshakeConn struct {
	net.Conn
	delay time.Duration
	once  sync.Once
}

func (c *slowHandshakeConn) Write(b []byte) (int, error) {
	c.once.Do(func() { time.Sleep(c.delay) })
	return c.Conn.Write(b)
}

func TestSlowHandshakesWithinWarmUpStillConnect(t *testing.T) {
	t.Parallel()
	// Every TLS handshake takes 3.5 s, as on a satellite link with a queue:
	// longer than any few-second cap, within warm-up. Connecting has
	// warm-up's budget, not a deadline of its own.
	e := &service{chunk: 64 << 10, chunkDelay: 15 * time.Millisecond}
	slow := httptest.NewUnstartedServer(e)
	slow.Listener = &slowHandshakes{Listener: slow.Listener, delay: 3500 * time.Millisecond}
	slow.StartTLS()
	t.Cleanup(slow.Close)
	cfg := testConfig(slow.URL)
	cfg.tls = slow.Client().Transport.(*http.Transport).TLSClientConfig
	if _, err := run(t.Context(), cfg, download, nil); err != nil {
		t.Fatal(err)
	}
}

func TestASlowConnectWithinWarmUpStillConnects(t *testing.T) {
	t.Parallel()
	// Every TCP connect takes 3.5 s, as on a lossy link that retransmits
	// its SYN: within warm-up, so the test runs.
	_, srv := newService(t, &service{chunkDelay: 15 * time.Millisecond})
	cfg := testConfig(srv.URL)
	cfg.test = &testHooks{dialControl: func(string, string, syscall.RawConn) error {
		time.Sleep(3500 * time.Millisecond)
		return nil
	}}
	if _, err := run(t.Context(), cfg, download, nil); err != nil {
		t.Fatal(err)
	}
}

func TestWarmUpThatRunsOutSaysSo(t *testing.T) {
	t.Parallel()
	// The service answers nothing within warm-up.
	_, srv := newService(t, &service{latency: 2 * time.Second})
	cfg := testConfig(srv.URL)
	cfg.warmup = time.Second
	if _, err := run(t.Context(), cfg, download, nil); !errors.Is(err, errConnectTimeout) {
		t.Fatalf("got %v, want %v", err, errConnectTimeout)
	}
}

func TestACancelledWarmUpIsNotATimeout(t *testing.T) {
	t.Parallel()
	_, srv := newService(t, &service{latency: 2 * time.Second})
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	_, err := run(ctx, testConfig(srv.URL), download, nil)
	if errors.Is(err, errConnectTimeout) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want the caller's own deadline", err)
	}
}

func TestNegotiatesHTTP1WithHTTP2Servers(t *testing.T) {
	t.Parallel()
	e := &service{chunk: 64 << 10, chunkDelay: 15 * time.Millisecond}
	srv := httptest.NewUnstartedServer(e)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	cfg := testConfig(srv.URL)
	cfg.tls = srv.Client().Transport.(*http.Transport).TLSClientConfig
	measure(t, cfg, download)
	if n := e.http2.Load(); n != 0 {
		t.Errorf("%d requests used HTTP/2", n)
	}
}

func TestChooseSize(t *testing.T) {
	largest := downloadSizes[len(downloadSizes)-1]
	for _, tc := range []struct {
		prev               int64
		rate               float64
		latency, remaining time.Duration
		want               int64
		ok                 bool
	}{
		{largest, 100e6, 50 * time.Millisecond, 8 * time.Second, 64 << 20, true},
		{largest, 65536 / 4.5, 10 * time.Millisecond, 3500 * time.Millisecond, 0, false},
		{largest, 65536 / 4.5, 10 * time.Millisecond, 8 * time.Second, 64 << 10, true},
		{largest, 10e6, 0, time.Second, 8 << 20, true},
		// 8 MiB would take 0.84 s plus 0.5 s of latency: past the time left.
		{largest, 10e6, 500 * time.Millisecond, time.Second, 1 << 20, true},
		// One quick small request is not proof the link carries 8 MiB a
		// second: grow one step at a time.
		{64 << 10, 100e6, 0, time.Second, 1 << 20, true},
	} {
		got, ok := chooseSize(downloadSizes, tc.prev, tc.rate, tc.latency, tc.remaining)
		if got != tc.want || ok != tc.ok {
			t.Errorf("chooseSize(%d, %g, %v, %v) = %d, %v; want %d, %v", tc.prev, tc.rate, tc.latency, tc.remaining, got, ok, tc.want, tc.ok)
		}
	}
}

func TestFormatMbps(t *testing.T) {
	for rate, want := range map[float64]string{-1: "0.0", 0: "0.0", 4.26: "4.3", 9.94: "9.9", 10: "10", 941.6: "942"} {
		if got := formatMbps(rate); got != want {
			t.Errorf("formatMbps(%g) = %q, want %q", rate, got, want)
		}
	}
}

func TestResultJSON(t *testing.T) {
	out, err := json.Marshal(result{Bytes: 1250000, Seconds: 1, Mbps: 10, Connections: 8})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"bytes":1250000,"seconds":1,"mbps":10,"connections":8}`; string(out) != want {
		t.Errorf("got %s, want %s", out, want)
	}
}

func TestBytesAfterTheWindowAreNotCounted(t *testing.T) {
	w := newWorker(testConfig("http://127.0.0.1:1"), download)
	w.win = newDownloadWindow(time.Second)
	w.win.closesAt(time.Now().Add(-2 * time.Second))
	err := w.receive(bytes.NewReader(make([]byte, 64<<10)), 64<<10, true)
	if err == nil || w.bytes.Load() != 0 {
		t.Errorf("err = %v, counted %d bytes after the window", err, w.bytes.Load())
	}
}

func TestAFailureCountsByWhetherTheWindowHadClosed(t *testing.T) {
	t.Parallel()
	bad := errors.New("bad acknowledgement")
	read := func() (int64, bool) { return 100, true }

	// Failed first: the close carries the failure, whatever order the run
	// loop reads them in.
	var before uploadClose
	if late := before.fail(bad); late {
		t.Error("a failure before the close was called late")
	}
	if c := before.close(read); !errors.Is(c.err, bad) {
		t.Errorf("the close carried %v, want the earlier failure", c.err)
	}

	// Closed first: the failure is late and the count stands.
	var after uploadClose
	if c := after.close(read); c.err != nil || c.bytes != 100 || !c.ok {
		t.Errorf("the close gave %+v, want the count", c)
	}
	if late := after.fail(bad); !late {
		t.Error("a failure after the close was not called late")
	}
}
