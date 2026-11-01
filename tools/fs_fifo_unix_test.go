//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package tools

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestLocalReadFileRejectsFIFO(t *testing.T) {
	root := t.TempDir()
	if err := syscall.Mkfifo(filepath.Join(root, "pipe"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("pipe", filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"pipe", "link"} {
		t.Run(name, func(t *testing.T) {
			done := make(chan error, 1)
			go func() {
				_, err := LocalFS(root).ReadFile(context.Background(), name, readMaxBytes)
				done <- err
			}()
			select {
			case err := <-done:
				if err == nil || !strings.Contains(err.Error(), "not a regular file") {
					t.Fatalf("ReadFile(%q) = %v; want non-regular-file error", name, err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("ReadFile blocked on FIFO")
			}
		})
	}
}

// Bypass the path check to model a regular file replaced by a FIFO between
// Stat and OpenFile. Opening must still return without waiting for a writer.
func TestLocalReadOpenDoesNotBlockOnSubstitutedFIFO(t *testing.T) {
	root := t.TempDir()
	if err := syscall.Mkfifo(filepath.Join(root, "pipe"), 0o600); err != nil {
		t.Fatal(err)
	}
	r, err := os.OpenRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	done := make(chan error, 1)
	go func() {
		f, err := openLocalReadFile(r, "pipe")
		if err != nil {
			done <- err
			return
		}
		defer f.Close()
		_, err = readLocalRegularFile(f, readMaxBytes)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "not a regular file") {
			t.Fatalf("substituted FIFO read = %v; want descriptor rejection", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("open or read blocked on substituted FIFO")
	}
}
