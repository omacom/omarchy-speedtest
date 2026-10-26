package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

// chunkReader hands out at most n bytes per Read, as a TLS connection hands
// out one record of up to 16 KiB at a time.
type chunkReader struct {
	left, n int64
}

func (c *chunkReader) Read(p []byte) (int, error) {
	if c.left == 0 {
		return 0, io.EOF
	}
	n := min(int64(len(p)), c.n, c.left)
	c.left -= n
	return int(n), nil
}

// BenchmarkReceive is the per-byte cost of counting a download, without the
// network: one Read per TLS record, the deadline check, the atomic add.
func BenchmarkReceive(b *testing.B) {
	const size = 64 << 20
	for _, record := range []int64{1 << 10, 16 << 10} {
		b.Run("record="+strconv.FormatInt(record>>10, 10)+"KiB", func(b *testing.B) {
			w := newWorker(testConfig("http://127.0.0.1:1"), download)
			w.win = newDownloadWindow(time.Hour)
			b.SetBytes(size)
			b.ReportAllocs()
			for b.Loop() {
				if err := w.receive(&chunkReader{left: size, n: record}, size, true); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkZeros is the cost of producing an upload body.
func BenchmarkZeros(b *testing.B) {
	buf := make([]byte, 64<<10)
	b.SetBytes(int64(len(buf)))
	for b.Loop() {
		z := zeros{left: int64(len(buf))}
		if _, err := z.Read(buf); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkChooseSize(b *testing.B) {
	for b.Loop() {
		chooseSize(uploadSizes, 1<<20, 15e6, 4*time.Millisecond, 3*time.Second)
	}
}

func BenchmarkAcknowledge(b *testing.B) {
	body := []byte(`{"acceptedBytes":8388608}`)
	w := newWorker(testConfig("http://127.0.0.1:1"), upload)
	b.ReportAllocs()
	for b.Loop() {
		if err := w.acknowledge(bytes.NewReader(body), 8<<20, false); err != nil {
			b.Fatal(err)
		}
	}
}

// fastService serves downloads and uploads as fast as loopback allows, so a
// round trip measures the client and the HTTP stack, not a link.
func fastService() http.Handler {
	payload := make([]byte, 64<<10)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/latency":
			w.WriteHeader(http.StatusNoContent)
		case "/down":
			size, _ := strconv.ParseInt(r.URL.Query().Get("bytes"), 10, 64)
			w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
			for left := size; left > 0; {
				n := min(int64(len(payload)), left)
				if _, err := w.Write(payload[:n]); err != nil {
					return
				}
				left -= n
			}
		case "/upload":
			n, _ := io.Copy(io.Discard, r.Body)
			_, _ = w.Write([]byte(`{"acceptedBytes":` + strconv.FormatInt(n, 10) + `}`))
		}
	})
}

// BenchmarkRoundTrip is one measured 8 MiB transfer on a warm connection,
// over plain HTTP and over TLS, server included. It bounds what one core
// can push through the client's HTTP path.
func BenchmarkRoundTrip(b *testing.B) {
	const size = 8 << 20
	for _, tc := range []struct {
		name string
		tls  bool
		dir  mode
	}{
		{"down/http", false, download},
		{"down/tls", true, download},
		{"up/http", false, upload},
		{"up/tls", true, upload},
	} {
		b.Run(tc.name, func(b *testing.B) {
			srv := httptest.NewUnstartedServer(fastService())
			if tc.tls {
				srv.StartTLS()
			} else {
				srv.Start()
			}
			defer srv.Close()
			cfg := testConfig(srv.URL)
			if tc.tls {
				cfg.tls = srv.Client().Transport.(*http.Transport).TLSClientConfig
			}
			w := newWorker(cfg, tc.dir)
			defer w.client.CloseIdleConnections()
			ctx, cancel := context.WithDeadline(b.Context(), time.Now().Add(time.Hour))
			defer cancel()
			if _, err := w.do(ctx, probe, 0, false); err != nil {
				b.Fatal(err)
			}
			b.SetBytes(size)
			b.ReportAllocs()
			for b.Loop() {
				if _, err := w.do(ctx, tc.dir, size, true); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
