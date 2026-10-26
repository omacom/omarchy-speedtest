package main

import (
	"bytes"
	"encoding/json/v2"
	"math"
	"slices"
	"testing"
	"time"
)

func FuzzChooseSize(f *testing.F) {
	f.Add(int64(64<<10), 100e6, int64(50*time.Millisecond), int64(8*time.Second))
	f.Add(int64(64<<20), 65536/4.5, int64(10*time.Millisecond), int64(3500*time.Millisecond))
	f.Add(int64(16<<10), 8000.0, int64(0), int64(5*time.Second))
	f.Fuzz(func(t *testing.T, prev int64, rate float64, latency, left int64) {
		if rate <= 0 || math.IsInf(rate, 0) || math.IsNaN(rate) || latency < 0 || left < 0 {
			t.Skip()
		}
		for _, sizes := range [][]int64{downloadSizes, uploadSizes} {
			size, ok := chooseSize(sizes, prev, rate, time.Duration(latency), time.Duration(left))
			fits := float64(sizes[0])/rate+time.Duration(latency).Seconds() <= time.Duration(left).Seconds()
			if ok != fits {
				t.Fatalf("ok = %v, but the smallest payload fits: %v", ok, fits)
			}
			if !ok {
				continue
			}
			if !slices.Contains(sizes, size) {
				t.Fatalf("size %d is not in %v", size, sizes)
			}
			if i, found := slices.BinarySearch(sizes, prev); found && size > sizes[min(i+1, len(sizes)-1)] {
				t.Fatalf("grew from %d to %d, more than one step", prev, size)
			}
		}
	})
}

func FuzzAcknowledge(f *testing.F) {
	f.Add([]byte(`{"acceptedBytes":1024}`), int64(1024))
	f.Add([]byte(`{"acceptedBytes":1023}`), int64(1024))
	f.Add([]byte(`{"bytes":1024}`), int64(1024))
	f.Add([]byte(`{"acceptedBytes":1024,"acceptedBytes":0}`), int64(1024))
	f.Add([]byte(`{"acceptedBytes":"1024"}`), int64(1024))
	f.Fuzz(func(t *testing.T, body []byte, size int64) {
		w := newWorker(testConfig("http://127.0.0.1:1"), upload)
		if err := w.acknowledge(bytes.NewReader(body), size, true); err != nil {
			if w.pending != 0 || w.acked != 0 {
				t.Fatalf("a rejected acknowledgement left %d pending, %d acked", w.pending, w.acked)
			}
			return
		}
		var ack struct {
			AcceptedBytes *int64 `json:"acceptedBytes"`
		}
		if len(body) > 4096 || json.Unmarshal(body, &ack) != nil || ack.AcceptedBytes == nil || *ack.AcceptedBytes != size {
			t.Fatalf("accepted %q for an upload of %d bytes", body, size)
		}
		if w.pending != size || w.bytes.Load() != 0 {
			t.Fatalf("pending %d, counted %d: an acknowledged upload waits for confirmation", w.pending, w.bytes.Load())
		}
	})
}
