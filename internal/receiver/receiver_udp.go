package receiver

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
)

type UDP struct {
	parser

	conn          net.Conn
	rawConn       syscall.RawConn
	mirrorUdpConn net.Conn
}

func listenPacket(network string, address string, fn func(int) error) (conn net.PacketConn, err error) {
	cfg := &net.ListenConfig{Control: func(network, address string, c syscall.RawConn) error {
		var err2 error
		err := c.Control(func(fd uintptr) {
			err2 = fn(int(fd))
		})
		if err != nil {
			return err
		}
		return err2
	}}
	conn, err = cfg.ListenPacket(context.Background(), network, address)
	return conn, err
}

func ListenUDP(network string, address string, bufferSize int, reusePort bool, sh2 *agent.Agent, mirrorUdpConn net.Conn, logPacket func(format string, args ...interface{})) (*UDP, error) {
	packetConn, err := listenPacket(network, address, func(fd int) error {
		setSocketReceiveBufferSize(fd, bufferSize)
		if reusePort {
			return syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, unix.SO_REUSEPORT, 1)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	conn := packetConn.(net.Conn)
	scConn := packetConn.(syscall.Conn)
	rawConn, err := scConn.SyscallConn()
	if err != nil {
		return nil, err
	}
	result := &UDP{
		parser:        parser{logPacket: logPacket, sh2: sh2, network: network},
		conn:          conn,
		rawConn:       rawConn,
		mirrorUdpConn: mirrorUdpConn,
	}
	result.parser.createMetrics()
	return result, nil
}

func (u *UDP) Duplicate() (*UDP, error) {
	var cf *os.File
	var err error
	if udpConn, ok := u.conn.(*net.UDPConn); ok {
		cf, err = udpConn.File()
	} else if unixConn, ok := u.conn.(*net.UnixConn); ok {
		cf, err = unixConn.File()
	} else {
		return nil, fmt.Errorf("must be UDP or Unix connection")
	}
	if err != nil {
		return nil, err
	}
	defer cf.Close() // File() and FileConn() both dup FD
	conn, err := net.FileConn(cf)
	if err != nil {
		return nil, err
	}
	scConn := conn.(syscall.Conn)
	rawConn, err := scConn.SyscallConn()
	if err != nil {
		return nil, err
	}
	result := &UDP{
		parser:        parser{logPacket: u.logPacket, sh2: u.sh2, network: u.network},
		conn:          conn,
		rawConn:       rawConn,
		mirrorUdpConn: u.mirrorUdpConn,
	}
	result.parser.createMetrics() // do not want receivers to collide on mutex inside builtin metric
	return result, nil
}

func (u *UDP) Close() error {
	return u.conn.Close()
}

func (u *UDP) Addr() string {
	return u.conn.LocalAddr().String()
}

func (u *UDP) Serve(h Handler) error {
	var batch tlstatshouse.AddMetricsBatchBytes
	var scratch []byte
	data := make([]byte, math.MaxUint16) // enough for any UDP packet
	for {
		pktLen, readErr := u.conn.Read(data)
		if readErr != nil {
			if errors.Is(readErr, net.ErrClosed) {
				return nil
			}
			return readErr
		}
		u.statPacketsTotal.Inc()
		u.statBytesTotal.Add(uint64(pktLen))
		pkt := data[:pktLen]
		if u.mirrorUdpConn != nil {
			_, _ = u.mirrorUdpConn.Write(pkt) // we intentionally ignore errors here
		}

		_ = u.parse(h, nil, nil, nil, pkt, &batch, &scratch) // ignore errors and read the next packet
	}
}

func (u *UDP) ReceiveBufferSize() int {
	res := -1 // Special valuein case of error
	_ = u.rawConn.Control(func(fd uintptr) {
		res, _ = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF)
	})
	return res
}
