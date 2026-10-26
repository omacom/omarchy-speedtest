# Security

## Reporting a vulnerability

Report it privately through GitHub's
[private vulnerability reporting](https://github.com/omacom/omarchy-speedtest/security/advisories/new),
not in a public issue. Fixes ship as a new release.

## What the client trusts

The client talks to one service, `https://speed.omarchy.org` unless
`OMARCHY_SPEEDTEST_URL` names another, over TLS with the system's
certificate roots. It sends zero-filled upload bodies and discards download
bodies after counting them; nothing it receives is stored, executed or
interpreted beyond a status code, a length and a small JSON acknowledgement
whose size is capped. It reads no files and needs no privileges.

`OMARCHY_SPEEDTEST_URL` is for development. Whoever can set the client's
environment can point it at another server, which then only learns the
client's address and sees test traffic.

## Releases

Every release is built by this repository's release workflow on GitHub's
hosted runners, from the tagged commit, reproducibly. Its files carry build
provenance and SBOM attestations, and `checksums.txt` a keyless cosign
signature; releases are immutable once published. The release notes show
how to verify them.
