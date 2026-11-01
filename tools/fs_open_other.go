//go:build !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris

package tools

import "os"

func openLocalReadFile(root *os.Root, name string) (*os.File, error) {
	return root.Open(name)
}
