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
  connection measures its request latency and grows its payload until a
  request takes 250 ms, then all start the five-second window together.
- Only payload counts, never other traffic on the machine. Downloads count
  body bytes as they arrive and stop counting when the window closes.
- An upload's acknowledgement does not prove its body arrived; the next
  request answered on the same connection does. So an upload counts once
  that request answers, and if the connection is replaced in between, its
  last upload is not counted. Upload requests are sized to take at most a
  second, start at 16 KiB so slow uplinks still finish one inside the
  window, and grow one size at a time.
- The result is the counted payload over the elapsed time: the window for
  downloads, and until the last upload is confirmed for uploads.

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
```

The tests run a local stand-in for the service (`speedtest_test.go`) and
check the result against what that server counted, with unrelated traffic
on the same link, slow and latent links, faults injected in the measured
phase, connection replacement and HTTP/2-capable servers. The process
tests (`main_test.go`, `parent_linux_test.go`) run the real `main`:
streamed progress, signals, a stale parent and, on Linux, parent death.

`packaging/PKGBUILD` is the Arch package recipe.
