//go:build !linux

package receiver

import (
	"syscall"

	"golang.org/x/sys/unix"
)

func setSocket(bufferSize int, reusePort bool) func(int) error {
	return func(fd int) error {
		for { // On Mac setting too large buffer is error, so we set the largest possible
			var err error
			if err = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, bufferSize); err == nil {
				break
			}
			if bufferSize == 0 {
				return err
			}
			bufferSize /= 2
		}
		if reusePort {
			return syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, unix.SO_REUSEPORT, 1)
		}
		return nil
	}

}
