package main

import "syscall"

// armParentDeathSignal asks the kernel to send SIGTERM when the parent
// exits. The setting belongs to the calling thread, which the Go runtime
// keeps for the life of the process because it is never locked to a
// goroutine.
func armParentDeathSignal() error {
	_, _, errno := syscall.RawSyscall(syscall.SYS_PRCTL, syscall.PR_SET_PDEATHSIG, uintptr(syscall.SIGTERM), 0)
	if errno != 0 {
		return errno
	}
	return nil
}
