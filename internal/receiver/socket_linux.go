//go:build linux

package receiver

import (
	"syscall"

	"golang.org/x/sys/unix"
)

func setSocket(bufferSize int, reusePort bool) func(int) error {
	return func(fd int) error {
		err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUFFORCE, bufferSize)
		if err != nil {
			// todo delete?
			if err = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, bufferSize); err != nil {
				return err
			}
		}
		if reusePort {
			return syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, unix.SO_REUSEPORT, 1)
		}
		return nil
	}
}
