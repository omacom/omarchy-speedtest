package main

import (
	"context"
	"crypto/tls"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultURL         = "https://speed.omarchy.org"
	defaultConnections = 8
	maxConnections     = 16
	userAgent          = "Omarchy-Speedtest/1.0 (+https://omarchy.org)"
)

// The service offers downloads of exactly these sizes and takes uploads of
// up to 64 MiB. Uploads start smaller so slow uplinks still finish a
// request inside the window, and stop at 8 MiB so each request stays short.
var (
	downloadSizes = []int64{64 << 10, 1 << 20, 8 << 20, 32 << 20, 64 << 20}
	uploadSizes   = []int64{16 << 10, 64 << 10, 1 << 20, 8 << 20}
)

type mode int

const (
	probe mode = iota
	download
	upload
)

type config struct {
	url         string
	tls         *tls.Config // nil for the system roots
	connections int
	warmup      time.Duration // connecting and warming every connection
	window      time.Duration // measured transfer time
	drain       time.Duration // extra time for the last uploads to be confirmed
	// Count uploads only by confirmed requests, even where the kernel can
	// say what was acknowledged. Tests use it to exercise that path.
	confirmedOnly bool
}

func defaultConfig() config {
	return config{
		url:         defaultURL,
		connections: defaultConnections,
		warmup:      6 * time.Second,
		window:      5 * time.Second,
		drain:       3 * time.Second,
	}
}

type result struct {
	Bytes       int64   `json:"bytes"`
	Seconds     float64 `json:"seconds"`
	Mbps        float64 `json:"mbps"`
	Connections int     `json:"connections"`
}

func mbps(bytes int64, elapsed time.Duration) float64 {
	if elapsed <= 0 {
		return 0
	}
	return float64(bytes) * 8 / elapsed.Seconds() / 1e6
}

// run measures one direction over cfg.connections persistent connections.
// It counts only payload bytes that belong to the measurement, never
// connection warm-up or other traffic on the machine, and calls progress
// with the running rate in Mbps until every connection has finished.
func run(ctx context.Context, cfg config, dir mode, progress func(float64)) (result, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type end struct {
		at  time.Time
		err error
	}
	workers := make([]*worker, cfg.connections)
	ready := make(chan error, cfg.connections)
	done := make(chan end, cfg.connections)
	gate := make(chan struct{})
	var started time.Time

	win := newDownloadWindow(cfg.window)
	warmupEnd := time.Now().Add(cfg.warmup)
	for i := range workers {
		w := newWorker(cfg, dir)
		w.win = win
		workers[i] = w
		go func() {
			defer w.client.CloseIdleConnections()
			if err := w.warm(ctx, warmupEnd); err != nil {
				ready <- err
				return
			}
			ready <- nil
			select {
			case <-gate:
			case <-ctx.Done():
				return
			}
			at, err := w.measure(ctx, started, cfg)
			done <- end{at, err}
		}()
	}

	warmup := time.NewTimer(time.Until(warmupEnd))
	defer warmup.Stop()
	for range cfg.connections {
		select {
		case err := <-ready:
			if err != nil {
				return result{}, err
			}
		case <-warmup.C:
			return result{}, errors.New("speed test connection timed out")
		}
	}

	confirmed := func() (n int64) {
		for _, w := range workers {
			n += w.bytes.Load()
		}
		return n
	}
	// Uploads count what the server's TCP acknowledged during the window,
	// where the kernel says, so a slow last answer cannot stretch the clock.
	var socket *socketCount
	if dir == upload && !cfg.confirmedOnly {
		socket = newSocketCount(workers, strings.HasPrefix(cfg.url, "https://"))
	}
	total := func() int64 {
		if n, ok := socket.delivered(); ok {
			return n
		}
		return confirmed()
	}

	started = time.Now()
	socket.start()
	close(gate)

	limit := time.NewTimer(cfg.window + cfg.drain + time.Second)
	defer limit.Stop()
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	windowEnd := time.NewTimer(cfg.window)
	defer windowEnd.Stop()
	ended := started
	for finished := 0; finished < cfg.connections; {
		select {
		case <-windowEnd.C:
			if n, ok := socket.delivered(); ok {
				if n == 0 {
					return result{}, errors.New("the connection is too slow to finish an upload in the test window")
				}
				return result{
					Bytes:       n,
					Seconds:     cfg.window.Seconds(),
					Mbps:        mbps(n, cfg.window),
					Connections: cfg.connections,
				}, nil
			}
		case e := <-done:
			if e.err != nil {
				return result{}, e.err
			}
			finished++
			if e.at.After(ended) {
				ended = e.at
			}
		case <-tick.C:
			if progress != nil {
				progress(mbps(total(), time.Since(started)))
			}
		case <-limit.C:
			return result{}, errors.New("speed test timed out")
		}
	}

	bytes := confirmed()
	if bytes == 0 && dir == upload {
		for _, w := range workers {
			if w.acked > 0 {
				return result{}, errors.New("the speed test service never confirmed an upload")
			}
		}
		return result{}, errors.New("the connection is too slow to finish an upload in the test window")
	}

	// Downloads count in one window from the first measured byte. Uploads
	// end when the server confirms the last byte.
	elapsed := ended.Sub(started)
	if dir == download {
		elapsed = cfg.window
	}
	return result{
		Bytes:       bytes,
		Seconds:     elapsed.Seconds(),
		Mbps:        mbps(bytes, elapsed),
		Connections: cfg.connections,
	}, nil
}

// A worker owns one persistent HTTP/1.1 connection. HTTP/1.1 keeps each
// worker on its own TCP connection with no HTTP/2 flow-control window
// between the payload and the link.
type worker struct {
	client *http.Client
	base   string
	dir    mode
	sizes  []int64
	bytes  atomic.Int64 // measured payload bytes
	reused atomic.Bool  // whether the last request reused the connection
	trace  httptrace.ClientTrace
	buf    []byte

	// An upload's acknowledgement does not prove its body arrived. The next
	// request answered on the same connection does, so the bytes wait here
	// until then.
	pending int64
	acked   int64 // measured upload bytes acknowledged, confirmed or not

	latency  time.Duration // a request on the warm connection
	warmSize int64
	warmTime time.Duration

	win *downloadWindow // shared by every download worker of a run

	// Upload accounting from the kernel: the current connection, and what
	// connections closed since the window opened had acknowledged.
	socketMu sync.Mutex
	conn     *countedConn
	counting bool
	retired  uint64
}

// errWindowClosed ends a download whose window closed mid-request.
var errWindowClosed = errors.New("measurement window closed")

// downloadWindow is the one window every download connection counts in. It
// opens at the first measured byte on any connection, so the wait for that
// byte is not counted against the link, and like any single window it holds
// no more than the link carried in it, however late a connection starts.
type downloadWindow struct {
	length time.Duration
	mu     sync.Mutex
	end    time.Time            // zero until the first measured byte
	cuts   []context.CancelFunc // the requests to cut when it closes
}

func newDownloadWindow(length time.Duration) *downloadWindow {
	return &downloadWindow{length: length}
}

// register adds a worker's request context to be cut when the window closes.
func (d *downloadWindow) register(cut context.CancelFunc) {
	d.mu.Lock()
	d.cuts = append(d.cuts, cut)
	d.mu.Unlock()
}

// closesAt opens the window at now if it is not open yet and returns its end.
func (d *downloadWindow) closesAt(now time.Time) time.Time {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.end.IsZero() {
		d.end = now.Add(d.length)
		time.AfterFunc(d.length, d.cutAll)
	}
	return d.end
}

// open reports whether the window has not closed yet.
func (d *downloadWindow) open() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.end.IsZero() || time.Now().Before(d.end)
}

func (d *downloadWindow) cutAll() {
	d.mu.Lock()
	cuts := d.cuts
	d.mu.Unlock()
	for _, cut := range cuts {
		cut()
	}
}

func newWorker(cfg config, dir mode) *worker {
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	transport := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		TLSClientConfig:     cfg.tls,
		TLSHandshakeTimeout: 3 * time.Second,
		MaxConnsPerHost:     1,
		MaxIdleConnsPerHost: 1,
		DisableCompression:  true,
		ReadBufferSize:      64 << 10,
		WriteBufferSize:     64 << 10,
		Protocols:           &protocols,
	}
	w := &worker{
		client: &http.Client{
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		base:  cfg.url,
		dir:   dir,
		sizes: downloadSizes,
		buf:   make([]byte, 64<<10),
	}
	if dir == upload {
		w.sizes = uploadSizes
	}
	dialer := &net.Dialer{Timeout: 3 * time.Second}
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := dialer.DialContext(ctx, network, address)
		tcp, ok := conn.(*net.TCPConn)
		if err != nil || !ok {
			return conn, err
		}
		counted := &countedConn{TCPConn: tcp, w: w}
		w.socketMu.Lock()
		w.conn = counted
		w.socketMu.Unlock()
		return counted, nil
	}
	w.trace.GotConn = func(info httptrace.GotConnInfo) { w.reused.Store(info.Reused) }
	return w
}

// warm opens the connection, measures its request latency and grows the
// payload until one request takes at least 250 ms or reaches 8 MiB.
func (w *worker) warm(ctx context.Context, deadline time.Time) error {
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	// The first probe pays for DNS, TCP and TLS; the second is a request on
	// the warm connection.
	if _, err := w.do(ctx, probe, 0, false); err != nil {
		return err
	}
	latency, err := w.do(ctx, probe, 0, false)
	if err != nil {
		return err
	}
	size := w.sizes[0]
	took, err := w.do(ctx, w.dir, size, false)
	if err != nil {
		return err
	}
	for _, next := range w.sizes[1:] {
		if took >= 250*time.Millisecond || next > 8<<20 {
			break
		}
		size = next
		if took, err = w.do(ctx, w.dir, size, false); err != nil {
			return err
		}
	}
	w.latency, w.warmSize, w.warmTime = latency, size, took
	if w.dir == upload {
		// Let the server take in the warm-up uploads before measuring.
		_, err = w.do(ctx, probe, 0, false)
	}
	return err
}

// measure transfers payload from started until the window closes and
// returns when this connection's share of the measurement ended.
func (w *worker) measure(ctx context.Context, started time.Time, cfg config) (time.Time, error) {
	windowEnd := started.Add(cfg.window)
	if w.dir == download {
		// Long streams keep the link busy. The shared window opens at the
		// first measured byte on any connection (receive), and the requests
		// in flight when it closes are cut.
		ctx, cancel := context.WithDeadline(ctx, windowEnd.Add(cfg.drain))
		defer cancel()
		w.win.register(cancel)
		for w.win.open() {
			if _, err := w.do(ctx, download, downloadSizes[len(downloadSizes)-1], true); err != nil {
				if w.win.open() {
					return time.Time{}, err
				}
				break
			}
		}
		return time.Now(), nil
	}

	ctx, cancel := context.WithDeadline(ctx, windowEnd.Add(cfg.drain))
	defer cancel()
	size := w.warmSize
	rate := float64(size) / max((w.warmTime-w.latency).Seconds(), 0.001)
	for time.Now().Before(windowEnd) {
		var ok bool
		if size, ok = chooseSize(w.sizes, size, rate, w.latency, time.Until(windowEnd)); !ok {
			break
		}
		took, err := w.do(ctx, upload, size, true)
		if err != nil {
			return time.Time{}, err
		}
		rate = float64(size) / max((took-w.latency).Seconds(), 0.001)
	}
	// The last upload is confirmed by one more request on its connection.
	if _, err := w.do(ctx, probe, 0, false); err != nil {
		return time.Time{}, err
	}
	return time.Now(), nil
}

// chooseSize returns the largest payload expected to take at most a second,
// so the last request ends close to the window, or the smallest payload if
// even that takes longer but still finishes in the time left. It grows at
// most one size past prev, since one quick request says little about the
// link. Request latency is kept out of the size estimate; it stays in the
// measured elapsed time, so it never inflates the reported rate.
func chooseSize(sizes []int64, prev int64, rate float64, latency, left time.Duration) (int64, bool) {
	if float64(sizes[0])/rate+latency.Seconds() > left.Seconds() {
		return 0, false
	}
	budget := rate * max(min(left, time.Second).Seconds()-latency.Seconds(), 0)
	limit := len(sizes) - 1
	if i, ok := slices.BinarySearch(sizes, prev); ok {
		limit = min(i+1, limit)
	}
	for _, size := range slices.Backward(sizes[1 : limit+1]) {
		if float64(size) <= budget {
			return size, true
		}
	}
	return sizes[0], true
}

func (w *worker) do(ctx context.Context, m mode, size int64, measured bool) (time.Duration, error) {
	method, path := http.MethodGet, "/latency"
	var body io.Reader
	switch m {
	case probe:
		// GET /latency, as set above.
	case download:
		path = "/down?bytes=" + strconv.FormatInt(size, 10)
	case upload:
		method, path = http.MethodPost, "/upload"
		body = &zeros{left: size}
	}
	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, &w.trace), method, w.base+path, body)
	if err != nil {
		return 0, err
	}
	if m == upload {
		req.ContentLength = size
	}
	phase := "warmup"
	if measured {
		phase = "measure"
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Speedtest-Phase", phase)

	begin := time.Now()
	resp, err := w.client.Do(req)
	if err != nil {
		return 0, err
	}
	// The body is read in full below, so closing it reports nothing useful.
	defer func() { _ = resp.Body.Close() }()
	want := http.StatusOK
	if m == probe {
		want = http.StatusNoContent
	}
	if resp.StatusCode != want {
		return 0, fmt.Errorf("unexpected speed test response: %s", resp.Status)
	}
	// This response proves the server received the previous upload, but
	// only if it came over the same connection.
	if w.reused.Load() {
		w.bytes.Add(w.pending)
	}
	w.pending = 0
	switch m {
	case probe:
		// A 204 has no body to read.
	case download:
		err = w.receive(resp.Body, size, measured)
	case upload:
		err = w.acknowledge(resp.Body, size, measured)
	}
	return time.Since(begin), err
}

// receive reads a download. The first measured byte of the run opens the
// shared window, and bytes that arrive after it closes are never counted.
func (w *worker) receive(body io.Reader, size int64, measured bool) error {
	var got int64
	for {
		n, err := body.Read(w.buf)
		if n > 0 {
			if measured {
				if now := time.Now(); !now.Before(w.win.closesAt(now)) {
					return errWindowClosed
				}
				w.bytes.Add(int64(n))
			}
			got += int64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	if got != size {
		return errors.New("incomplete test download")
	}
	return nil
}

// acknowledge checks the server accepted the whole upload and holds a
// measured one until the next request on the connection confirms it.
func (w *worker) acknowledge(body io.Reader, size int64, measured bool) error {
	reply, err := io.ReadAll(io.LimitReader(body, 4097))
	if err != nil {
		return err
	}
	var ack struct {
		AcceptedBytes *int64 `json:"acceptedBytes"`
	}
	if len(reply) > 4096 || json.Unmarshal(reply, &ack) != nil {
		return errors.New("invalid upload acknowledgement")
	}
	if ack.AcceptedBytes == nil || *ack.AcceptedBytes != size {
		return errors.New("server did not acknowledge the complete upload")
	}
	if measured {
		w.pending = size
		w.acked += size
	}
	return nil
}

// zeros is an upload body of left zero bytes.
type zeros struct{ left int64 }

func (z *zeros) Read(p []byte) (int, error) {
	if z.left == 0 {
		return 0, io.EOF
	}
	n := int(min(int64(len(p)), z.left))
	clear(p[:n])
	z.left -= int64(n)
	return n, nil
}

// countedConn records, as it closes, what the server acknowledged on it,
// so a connection replaced mid-window loses none of its count.
type countedConn struct {
	*net.TCPConn
	w    *worker
	base uint64 // acknowledged before the window opened
	gone bool
}

func (c *countedConn) Close() error {
	c.w.socketMu.Lock()
	if c.w.counting && !c.gone {
		if acked, ok := bytesAcked(c.TCPConn); ok {
			c.w.retired += acked - c.base
		}
	}
	c.gone = true
	c.w.socketMu.Unlock()
	return c.TCPConn.Close()
}

// socketCount reads how many bytes the server's TCP has acknowledged on
// every worker's connections since the window opened.
type socketCount struct {
	workers []*worker
	tls     bool
	ok      bool
}

func newSocketCount(workers []*worker, tls bool) *socketCount {
	return &socketCount{workers: workers, tls: tls}
}

func (s *socketCount) start() {
	if s == nil {
		return
	}
	s.ok = true
	for _, w := range s.workers {
		w.socketMu.Lock()
		if w.conn == nil {
			s.ok = false
		} else if acked, ok := bytesAcked(w.conn.TCPConn); ok {
			w.conn.base = acked
		} else {
			s.ok = false
		}
		w.counting, w.retired = true, 0
		w.socketMu.Unlock()
	}
}

// TLS 1.3 wraps each full 16 KiB record in 22 bytes of framing; the HTTP
// request lines add about 200 bytes per upload, which is not removed.
const tlsRecordPayload, tlsRecordOverhead = 16384, 22

// delivered returns the payload acknowledged since start, or false where
// the kernel cannot say.
func (s *socketCount) delivered() (int64, bool) {
	if s == nil || !s.ok {
		return 0, false
	}
	var n uint64
	for _, w := range s.workers {
		w.socketMu.Lock()
		n += w.retired
		if c := w.conn; c != nil && !c.gone {
			acked, ok := bytesAcked(c.TCPConn)
			if !ok {
				w.socketMu.Unlock()
				return 0, false
			}
			n += acked - c.base
		}
		w.socketMu.Unlock()
	}
	if s.tls {
		n = n * tlsRecordPayload / (tlsRecordPayload + tlsRecordOverhead)
	}
	return int64(n), true
}
