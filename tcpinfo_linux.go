package main

import (
	"encoding/binary"
	"net"
	"syscall"
	"unsafe"
)

// bytesAcked returns how many bytes the peer has acknowledged on conn:
// tcpi_bytes_acked from TCP_INFO, at byte 120 of struct tcp_info since
// Linux 4.1. It reports false where the kernel does not provide it.
func bytesAcked(conn *net.TCPConn) (uint64, bool) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return 0, false
	}
	var info [256]byte
	size := uint32(len(info))
	var errno syscall.Errno
	err = raw.Control(func(fd uintptr) {
		//nolint:gosec // getsockopt writes into info and size; both outlive the call
		_, _, errno = syscall.Syscall6(syscall.SYS_GETSOCKOPT, fd, syscall.IPPROTO_TCP, syscall.TCP_INFO,
			uintptr(unsafe.Pointer(&info[0])), uintptr(unsafe.Pointer(&size)), 0)
	})
	if err != nil || errno != 0 || size < 128 {
		return 0, false
	}
	return binary.NativeEndian.Uint64(info[120:128]), true
}
