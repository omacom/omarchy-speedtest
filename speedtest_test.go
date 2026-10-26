package main

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// service is a stand-in for the speed test service. It records what it
// served in the measured phase so a test can compare the client's result
// with the server's own count.
type service struct {
	latency    time.Duration // before every response
	chunk      int           // download write size
	chunkDelay time.Duration // after every download write
	readDelay  time.Duration // after every 64 KiB of upload read
	upRate     float64       // upload bytes per second per connection, if set
	truncate   bool          // send half of every measured download
	badAck     bool          // acknowledge one byte less than a measured upload
	closeAfter bool          // close the connection after measured uploads
	closeOnce  bool          // close one connection after its third measured upload
	shortBody  bool          // honestly send half of every measured download
	earlyAck   bool          // acknowledge measured uploads before reading them, then close

	mu          sync.Mutex
	measured    int64     // payload bytes in the measured phase
	first, last time.Time // first measured request, last measured byte
	uploads     []int64   // measured upload sizes
	perConn     map[string]int
	dropped     int64 // the upload whose connection closeOnce closed

	closed   atomic.Bool
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
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		switch state {
		case http.StateNew:
			e.open.Add(1)
		case http.StateClosed, http.StateHijacked:
			e.open.Add(-1)
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
			if measured {
				e.mark(n)
			}
		})
		if truncate {
			panic(http.ErrAbortHandler)
		}
	case "/upload":
		if measured {
			e.mark(0)
			e.mu.Lock()
			e.uploads = append(e.uploads, r.ContentLength)
			e.mu.Unlock()
		}
		if measured && e.earlyAck {
			// Acknowledge before reading the body, then read it.
			http.NewResponseController(w).EnableFullDuplex()
			w.Header().Set("Connection", "close")
			w.Write([]byte(`{"acceptedBytes":` + strconv.FormatInt(r.ContentLength, 10) + `}`))
			http.NewResponseController(w).Flush()
			io.Copy(io.Discard, r.Body)
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
			if e.upRate > 0 {
				time.Sleep(time.Duration(float64(n) / e.upRate * float64(time.Second)))
			}
			if err != nil {
				break
			}
			time.Sleep(e.readDelay)
		}
		if got != r.ContentLength {
			return
		}
		if measured {
			e.mark(got)
		}
		if e.badAck && measured {
			got--
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
		w.Write([]byte(`{"acceptedBytes":` + strconv.FormatInt(got, 10) + `}`))
	default:
		http.NotFound(w, r)
	}
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
	for _, dir := range []mode{download, upload} {
		name := map[mode]string{download: "down", upload: "up"}[dir]
		t.Run(name, func(t *testing.T) {
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
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
			})
			res, steps := measure(t, testConfig(srv.URL), dir)
			stop()
			wg.Wait()

			e.mu.Lock()
			served, elapsed := e.measured, e.last.Sub(e.first)
			e.mu.Unlock()
			want := mbps(served, elapsed)
			if res.Mbps < 0.8*want || res.Mbps > 1.2*want {
				t.Errorf("rate %.1f Mbps, server saw %.1f Mbps", res.Mbps, want)
			}
			if dir == upload && res.Bytes != served {
				t.Errorf("counted %d upload bytes, server acknowledged %d", res.Bytes, served)
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

func TestVerySlowUplinksStillMeasure(t *testing.T) {
	t.Parallel()
	// 512 kbit/s shared by eight connections: 8 kB/s each, so a 64 KiB
	// request would take 8 s, longer than warm-up and the window.
	_, srv := newService(t, &service{upRate: 8000})
	if res, _ := measure(t, testConfig(srv.URL), upload); res.Bytes == 0 {
		t.Error("no bytes measured")
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
		name string
		svc  *service
		dir  mode
		want string
	}{
		{"short acknowledgement", &service{badAck: true}, upload, "acknowledge"},
		{"truncated download", &service{truncate: true}, download, "EOF"},
		{"uploads never confirmed on their connection", &service{closeAfter: true}, upload, "confirm"},
		// Acknowledgements that arrive before the body make request times
		// meaningless, so which check stops the test varies: any error will do,
		// a result will not.
		{"early acknowledgement on a closing connection", &service{earlyAck: true}, upload, ""},
		// 3.2 kB/s per connection: warm-up finishes, but no 16 KiB upload fits
		// the window. A working link must not read as zero.
		{"uplink too slow for the window", &service{upRate: 3200}, upload, "too slow"},
		{"download shorter than requested", &service{shortBody: true}, download, "incomplete"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, srv := newService(t, tc.svc)
			_, err := run(t.Context(), testConfig(srv.URL), tc.dir, nil)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

func TestReplacedConnectionDropsOnlyItsUnconfirmedUpload(t *testing.T) {
	t.Parallel()
	// The replaced connection has already had two uploads confirmed; only
	// the one its replacement leaves unconfirmed may go uncounted.
	e, srv := newService(t, &service{closeOnce: true, readDelay: 15 * time.Millisecond})
	res, _ := measure(t, testConfig(srv.URL), upload)
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
	ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Millisecond))
	defer cancel()
	w := newWorker(testConfig("http://127.0.0.1:1"), download)
	err := w.receive(ctx, bytes.NewReader(make([]byte, 64<<10)), 64<<10, true)
	if err == nil || w.bytes.Load() != 0 {
		t.Errorf("err = %v, counted %d bytes after the window", err, w.bytes.Load())
	}
}
