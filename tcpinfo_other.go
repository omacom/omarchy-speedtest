//go:build !linux

package main

import "net"

// bytesAcked is Linux-only; elsewhere uploads count confirmed requests.
func bytesAcked(*net.TCPConn) (uint64, bool) { return 0, false }
