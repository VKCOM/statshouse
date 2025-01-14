//go:build ignore

package rpc

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"github.com/stretchr/testify/require"
)

type pcapEndpoint struct {
	host string
	port layers.TCPPort
}

func (e pcapEndpoint) Network() string {
	return "ip"
}

func (e pcapEndpoint) String() string {
	return fmt.Sprintf("%s:%d", e.host, e.port)
}

type testConn struct {
	localAddr  net.Addr
	remoteAddr net.Addr
	buffer     []byte
	offset     int
}

func (c *testConn) Read(b []byte) (n int, err error) {
	if c.offset == len(c.buffer) {
		return 0, io.EOF
	}
	n = copy(b, c.buffer[c.offset:])
	c.offset += n
	return n, nil
}

func (c *testConn) Write(b []byte) (int, error) {
	return len(b), nil // nop
}

func (c *testConn) Close() error {
	c.offset = len(c.buffer)
	return nil
}

func (c *testConn) LocalAddr() net.Addr {
	return c.localAddr
}

func (c *testConn) RemoteAddr() net.Addr {
	return c.remoteAddr
}

func (c *testConn) SetDeadline(_ time.Time) error {
	return nil
}

func (c *testConn) SetReadDeadline(_ time.Time) error {
	return nil
}

func (c *testConn) SetWriteDeadline(_ time.Time) error {
	return nil
}

// NB! remove "received unexpected pong" assertion fot test to pass
func TestPlayPcap(t *testing.T) {
	path := os.Getenv("STATSHOUSE_TEST_PLAY_PCAP_FILE_PATH")
	if path == "" {
		return
	}
	log.Println("PCAP play", path)
	for k, v := range readPCAP(t, path, "") {
		playPcap(t, k, v)
	}
}

func playPcap(t *testing.T, k [2]pcapEndpoint, v []byte) {
	if len(v) == 0 {
		return
	}
	srcConn := &testConn{
		buffer:     v,
		localAddr:  k[0],
		remoteAddr: k[1],
	}
	src := &PacketConn{
		conn:            srcConn,
		timeoutAccuracy: DefaultConnTimeoutAccuracy,
		r:               newCryptoReader(srcConn, DefaultServerRequestBufSize),
		w:               newCryptoWriter(srcConn, DefaultServerResponseBufSize),
		readSeqNum:      int64(binary.LittleEndian.Uint32(v[4:])),
		table:           castagnoliTable,
	}
	dstConn := &testConn{
		localAddr:  k[0],
		remoteAddr: k[1],
	}
	dst := &PacketConn{
		conn:            dstConn,
		timeoutAccuracy: DefaultConnTimeoutAccuracy,
		r:               newCryptoReader(dstConn, DefaultServerRequestBufSize),
		w:               newCryptoWriter(dstConn, DefaultServerResponseBufSize),
		table:           castagnoliTable,
	}
	var buf PacketHeaderCircularBuffer
	for i := 0; ; i++ {
		res := ForwardPacket(dst, src, forwardPacketOptions{testEnv: true})
		buf.add(res.packetHeader)
		if res.Error() != nil {
			require.ErrorIsf(t, res.ReadErr, io.EOF, "#%d %v %s", i, k, buf.String())
			require.NoError(t, res.WriteErr)
			break
		}
	}
}

func readPCAP(t *testing.T, path string, dstHost string) map[[2]pcapEndpoint][]byte {
	handle, err := pcap.OpenOffline(path)
	require.NoError(t, err)
	packetSource := gopacket.NewPacketSource(handle, handle.LinkType())
	m := map[[2]pcapEndpoint][]byte{}
	for p := range packetSource.Packets() {
		var src, dst pcapEndpoint
		ip, ok := p.Layer(layers.LayerTypeIPv4).(*layers.IPv4)
		if !ok {
			continue
		}
		src.host = ip.SrcIP.String()
		dst.host = ip.DstIP.String()
		if dstHost != "" && dst.host != dstHost {
			continue
		}
		if tcp, _ := p.Layer(layers.LayerTypeTCP).(*layers.TCP); tcp != nil {
			src.port = tcp.SrcPort
			dst.port = tcp.DstPort
		}
		if app := p.ApplicationLayer(); app != nil {
			k := [2]pcapEndpoint{src, dst}
			m[k] = append(m[k], app.Payload()...)
		}
	}
	return m
}
