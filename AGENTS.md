# Working on omarchy-speedtest

omarchy-speedtest is the client behind `omarchy network speedtest` and the
speed test panel in Omarchy. It measures one direction at a time against
`speed.omarchy.org` and prints Mbps, one number per line. Read README.md
first: it is the reference for how the measurement works.

## Principles

- **Count only what was measured.** A result is test payload over the
  window: never connection setup, warm-up, other traffic on the machine,
  bytes after the window closes, or an upload the server has not
  confirmed. Every change that touches counting or timing comes with a
  test that compares the result against the stand-in server's own count.
- **Never break the panel.** Omarchy's launcher `exec`s this binary and the
  panel reads its stdout: running rates, then the final rate, one number
  per line; exit 0 with a result, 1 on failure, 2 on a usage error. Keep
  that contract, and `--json` and the `OMARCHY_SPEEDTEST_*` variables, as
  they are.
- **Standard library only.** No dependencies in the client's module.
  Tools live in `tools/go.mod` and run with `go tool -modfile=tools/go.mod`.
- **HTTP/1.1, one connection per worker.** HTTP/2 would put every worker
  on one TCP connection behind one flow-control window.
- **A closed panel stops every transfer.** Parent-death handling and the
  launcher PID check are tested on Linux; keep them that way.

## Changes

- Write the test first and watch it fail for the right reason, then make
  it pass. A guard without a test that fails when the guard is removed is
  untested.
- Measure before claiming a speed or cost change: `go test -bench`, and
  real runs against the service with CPU and context-switch counts.
- Run before pushing:

  ```sh
  go test -race -shuffle=on ./...
  golangci-lint run
  go tool -modfile=tools/go.mod govulncheck ./...
  go tool -modfile=tools/go.mod actionlint
  ```

- Commit subjects are one imperative sentence without a prefix or a
  trailing period; the body says why.
- Actions are pinned to full commit SHAs. Dependabot updates them and the
  Go tools after a cooldown. The golangci-lint, GoReleaser, cosign, syft
  and zizmor versions sit in each workflow's `env:` and are bumped by hand.

## Releases

Tag `vX.Y.Z` on `main` and push the tag, then approve the run in the
`release` environment; the release workflow does the rest. A published
release is immutable: a mistake is fixed by the next version, never by
replacing files. omarchy-pkgs watches this repository's releases and proposes the
package update itself.
