//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package tools

import (
	"os"
	"syscall"
)

// O_NONBLOCK prevents a FIFO substituted after the path check from blocking
// the open. It has no effect on regular files. The caller checks f.Stat before
// reading, so a substituted special file is rejected rather than read.
func openLocalReadFile(root *os.Root, name string) (*os.File, error) {
	return root.OpenFile(name, os.O_RDONLY|syscall.O_NONBLOCK, 0)
}
