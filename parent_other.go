//go:build !linux

package main

// armParentDeathSignal is Linux-only; elsewhere the parent check in main is
// all there is.
func armParentDeathSignal() error { return nil }
