# omarchy-speedtest

Omarchy's network speed test client. It measures download or upload speed
against `speed.omarchy.org` and is what `omarchy network speedtest` and the
speed test panel run. Omarchy installs it as `/usr/lib/omarchy/speedtest`.

```sh
omarchy-speedtest down|up [--json]
```

Without `--json` it prints the running rate in Mbps about four times a
second and the final rate last, one number per line; the panel reads this.
With `--json` it prints one record:

```json
{"bytes":577238143,"seconds":5,"mbps":923.58,"connections":8}
```

It exits 0 with a result, 1 when the test fails and 2 for a usage error.

## How it measures

- Eight persistent HTTP/1.1 connections, one goroutine each. HTTP/1.1 keeps
  every worker on its own TCP connection, with no HTTP/2 flow-control
  window between the payload and the link.
- Connecting (DNS, TCP, TLS) and warm-up stay outside the measurement. Each
  connection measures its request latency within warm-up's six seconds, with
  no shorter timeout of its own, so slow handshakes still connect. Upload
  connections also grow their payload until a request takes 250 ms, then the
  five-second window starts. Download connections skip that: they stream
  the largest payload until the window closes, and warm-up transfers would
  only take a slow link's time.
- Only payload counts, never other traffic on the machine.
- Downloads count body bytes as they arrive, in one window that opens at
  the first measured byte on any connection, so the wait for that byte does
  not count against the link. The requests in flight when it closes are
  cut. The result divides by the window, and one window can hold no more
  than the link carried in it, however late a connection starts.
  A first byte later than the three-second drain allowance ends the run
  with an error that says so.
- Uploads, on Linux, count what the server's TCP acknowledged during the
  window: `tcpi_bytes_acked` from `TCP_INFO`, read for every connection as
  the window opens and as it closes, and as a connection closes in between.
  The closing read runs on the window's own timer, and progress is
  reported from its own goroutine, so a reader that falls behind cannot
  change the result.
  TLS record framing is taken off; the request lines, about 200 bytes per
  upload, stay in. The result divides by the window, so a slow last answer
  from the server cannot stretch the clock.
- Elsewhere, and through an HTTP proxy, whose acknowledgements say nothing
  about the service, uploads count confirmed requests: an acknowledgement does not
  prove the body arrived, but the next request answered on the same
  connection does, and the clock runs until the last one answers.
- Upload requests are sized to take at most a second, start at 16 KiB so
  slow uplinks still finish one inside the window, and grow one size at a
  time.

On Linux the client asks for `SIGTERM` when its parent exits
(`PR_SET_PDEATHSIG`), and exits if its parent is no longer the process in
`OMARCHY_SPEEDTEST_PARENT_PID`, so a closed panel never leaves transfers
running.

## Environment

| Variable | Use |
|---|---|
| `OMARCHY_SPEEDTEST_PARENT_PID` | The launcher's parent, set by `omarchy-network-speedtest` |
| `OMARCHY_SPEEDTEST_URL` | Another compatible service, for development |
| `OMARCHY_SPEEDTEST_CONNECTIONS` | 1 to 16 connections instead of 8, for comparisons |

## Building and testing

It needs Go 1.27 and nothing outside the standard library.

```sh
go build -trimpath -ldflags='-s -w' -o omarchy-speedtest .
go test -race ./...
go test -run='^$' -fuzz='^FuzzChooseSize$' -fuzztime=30s .
```

The tests run a local stand-in for the service (`speedtest_test.go`) and
check the result against what that server counted, with unrelated traffic
on the same link, slow and latent links, faults injected in the measured
phase, connection replacement and HTTP/2-capable servers. The process
tests (`main_test.go`, `parent_linux_test.go`) run the real `main`:
streamed progress, signals, a stale parent and, on Linux, parent death.
The fuzz tests (`fuzz_test.go`) hold the upload sizing and the
acknowledgement parser to their invariants.

CI runs the tests with the race detector on x86_64 and aarch64,
golangci-lint, govulncheck, the fuzz tests, actionlint and zizmor on the
workflows, and a dry run of the release; the `ci` check gathers them.

## Releases

Pushing a `v*` tag runs the same checks, then waits for a maintainer to
approve the `release` environment. GoReleaser then builds static x86_64 and
aarch64 binaries, reproducibly, with an SPDX SBOM per archive and a keyless
cosign signature over `checksums.txt`. The build and its provenance and
SBOM attestations come from an isolated reusable workflow
(`release-build.yml`), which meets SLSA v1.0 Build Level 3. The
attestations are attached before the release is published, and releases
are immutable from then on. Each release says how to verify it.
Omarchy's package builds from the tagged source in
[omarchy-pkgs](https://github.com/omacom/omarchy-pkgs).
