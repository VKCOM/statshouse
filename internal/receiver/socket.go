//go:build !linux

package receiver

import (
	"syscall"
)

func setSocketBufferSize(fd int, bufferSize int) {
	if syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, bufferSize) != nil {
		// Either we don't have CAP_NET_ADMIN priviledge to set SO_RCVBUFFORCE
		// or buffer size is beyond configured system limit.
		// Trying to set the largest value possible.
		var curr int
		if n, err := syscall.GetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF); err == nil {
			curr = n
		}
		for bufferSize /= 2; curr < bufferSize; bufferSize /= 2 {
			if syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, bufferSize) == nil {
				break
			}

		}
	}
}
