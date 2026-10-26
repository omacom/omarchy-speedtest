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

	warmupEnd := time.Now().Add(cfg.warmup)
	for i := range workers {
		w := newWorker(cfg, dir)
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

	total := func() (n int64) {
		for _, w := range workers {
			n += w.bytes.Load()
		}
		return n
	}

	started = time.Now()
	close(gate)

	limit := time.NewTimer(cfg.window + cfg.drain + time.Second)
	defer limit.Stop()
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	ended := started
	for finished := 0; finished < cfg.connections; {
		select {
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

	bytes := total()
	if bytes == 0 && dir == upload {
		for _, w := range workers {
			if w.acked > 0 {
				return result{}, errors.New("the speed test service never confirmed an upload")
			}
		}
		return result{}, errors.New("the connection is too slow to finish an upload in the test window")
	}

	// A download's last request is cut at the window, so the window is its
	// elapsed time. Uploads end when the server confirms the last byte.
	if dir == download {
		if windowEnd := started.Add(cfg.window); ended.After(windowEnd) {
			ended = windowEnd
		}
	}
	elapsed := ended.Sub(started)
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
}

func newWorker(cfg config, dir mode) *worker {
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	transport := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		DialContext:         (&net.Dialer{Timeout: 3 * time.Second}).DialContext,
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
		// Long streams keep the link busy. The last request is cut at the
		// window, and bytes that arrive after it are not counted.
		ctx, cancel := context.WithDeadline(ctx, windowEnd)
		defer cancel()
		for time.Now().Before(windowEnd) {
			if _, err := w.do(ctx, download, downloadSizes[len(downloadSizes)-1], true); err != nil {
				if time.Now().Before(windowEnd) {
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
	defer resp.Body.Close()
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
	case download:
		err = w.receive(ctx, resp.Body, size, measured)
	case upload:
		err = w.acknowledge(resp.Body, size, measured)
	}
	return time.Since(begin), err
}

// receive reads a download, counting measured bytes only while the
// request's context is live, so bytes after the window are never counted.
func (w *worker) receive(ctx context.Context, body io.Reader, size int64, measured bool) error {
	deadline, bounded := ctx.Deadline()
	var got int64
	for {
		n, err := body.Read(w.buf)
		if n > 0 {
			if measured && bounded && !time.Now().Before(deadline) {
				return context.DeadlineExceeded
			}
			got += int64(n)
			if measured {
				w.bytes.Add(int64(n))
			}
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
