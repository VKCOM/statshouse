package rpc

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"pgregory.net/rapid"
)

type cryptoPipelineMachine struct {
	r        *cryptoReader
	w        *cryptoWriter
	rb       *bytes.Buffer
	actual   *bytes.Buffer
	expected []byte
	offset   int
	fatalf   func(format string, args ...any)
}

func (c *cryptoPipelineMachine) Write(t *rapid.T) {
	s := rapid.SliceOf(rapid.Byte()).Draw(t, "slice")
	c.expected = append(c.expected, s...)
	for len(s) != 0 {
		n, err := c.rb.Write(s)
		if err != nil {
			c.fatalf("write failed: %v", err)
		}
		s = s[n:]
	}
}

func (c *cryptoPipelineMachine) ReadDiscard(t *rapid.T) {
	n := rapid.IntRange(0, c.rb.Len()).Draw(t, "n")
	m, err := c.r.Read(make([]byte, n))
	if err != nil {
		c.fatalf("Read failed: %v", err)
	}
	c.expected = append(c.expected[:c.offset], c.expected[c.offset+m:]...)
}

func (c *cryptoPipelineMachine) Copy(t *rapid.T) {
	n := rapid.IntRange(0, c.rb.Len()).Draw(t, "n")
	_, rwe := cryptoCopy(c.w, c.r, n, 0, nil, nil)
	if rwe.Error() != nil {
		c.fatalf("copy failed: %v, %v", rwe.WriteErr, rwe.ReadErr)
	}
	if err := c.w.Flush(); err != nil {
		c.fatalf("flush failed: %v", err)
	}
	c.offset += n
}

func (c *cryptoPipelineMachine) Check(_ *rapid.T) {
	if len(c.expected) < c.actual.Len() {
		c.fatalf("expected %v bytes, actual %v bytes", len(c.expected), c.actual.Len())
	}
	expected := c.expected[:c.actual.Len()]
	actual := c.actual.Bytes()
	if !bytes.Equal(expected, actual) {
		c.fatalf("expected %q, actual %q", expected, actual)
	}
}

func TestCryptoPipeline(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		rb := &bytes.Buffer{}
		actual := &bytes.Buffer{}
		c := &cryptoPipelineMachine{
			rb:     rb,
			actual: actual,
			r:      newCryptoReader(rb, rapid.IntRange(0, 1024).Draw(t, "read_buffer_size")),
			w:      newCryptoWriter(actual, rapid.IntRange(0, 1024).Draw(t, "write_buffer_size")),
			fatalf: t.Fatalf,
		}
		t.Repeat(rapid.StateMachineActions(c))
		_, _ = cryptoCopy(c.w, c.r, c.rb.Len()+cap(c.r.buf), 0, nil, nil)
		_ = c.w.Flush()
		if !bytes.Equal(c.expected, c.actual.Bytes()) {
			c.fatalf("expected %q, actual %q", c.expected, c.actual.Bytes())
		}
	})
}

type forwardPacketMachine struct {
	encryptClient  bool
	encryptServer  bool
	protocolClient uint32
	protocolServer uint32

	client      *PacketConn
	server      *PacketConn
	proxyClient *PacketConn
	proxyServer *PacketConn
}

func newForwardPacketMachine(t *rapid.T) (_ forwardPacketMachine, _ func(), err error) {
	startTime := uint32(time.Now().Unix())
	cryptoKey := "crypto_key"
	handshakeServer := func(conn net.Conn, forceEncryption bool) (*PacketConn, error) {
		res := NewPacketConn(conn, DefaultServerRequestBufSize, DefaultServerResponseBufSize)
		_, _, err := res.HandshakeServer([]string{cryptoKey}, nil, forceEncryption, startTime, 0)
		return res, err
	}
	handshakeClient := func(conn net.Conn, version uint32, forceEncryption bool) (*PacketConn, error) {
		res := NewPacketConn(conn, DefaultServerRequestBufSize, DefaultServerResponseBufSize)
		err := res.HandshakeClient(cryptoKey, nil, forceEncryption, startTime, 0, 0, version)
		return res, err
	}
	protocolVersion := func(label string) uint32 {
		if rapid.Bool().Draw(t, label) {
			return DefaultProtocolVersion
		} else {
			return LatestProtocolVersion
		}
	}
	client, proxyServer := net.Pipe()
	proxyClient, server := net.Pipe()
	cancel := func() {
		client.Close()
		proxyServer.Close()
		proxyClient.Close()
		server.Close()
	}
	defer func() {
		if err != nil {
			cancel()
		}
	}()
	res := forwardPacketMachine{
		encryptClient:  rapid.Bool().Draw(t, "encrypt_client"),
		encryptServer:  rapid.Bool().Draw(t, "encrypt_server"),
		protocolClient: protocolVersion("protocol_client"),
		protocolServer: protocolVersion("protocol_proxy"),
	}
	var group errgroup.Group
	group.Go(func() (err error) {
		res.server, err = handshakeServer(server, res.encryptServer)
		return err
	})
	group.Go(func() (err error) {
		if res.proxyServer, err = handshakeServer(proxyServer, res.encryptClient); err == nil {
			res.proxyClient, err = handshakeClient(proxyClient, res.protocolServer, res.encryptServer)
		}
		return err
	})
	if res.client, err = handshakeClient(client, res.protocolClient, res.encryptClient); err == nil {
		err = group.Wait()
	}
	return res, cancel, err
}

func (m *forwardPacketMachine) run(t *rapid.T) {
	type message struct {
		tip  uint32
		body []byte
	}
	minBodyLen := 1
	legacyProtocol := m.protocolClient == 0
	if legacyProtocol {
		minBodyLen = 4
	}
	for i := 0; i < 512; i++ {
		var forward errgroup.Group
		forward.Go(func() error {
			res := ForwardPacket(m.proxyClient, m.proxyServer, forwardPacketOptions{testEnv: false})
			return res.Error()
		})
		sent := message{
			tip:  0x1234567,
			body: rapid.SliceOfN(rapid.Byte(), minBodyLen, 1024).Draw(t, "body"),
		}
		if legacyProtocol {
			sent.body = sent.body[:len(sent.body)-len(sent.body)%4]
		}
		err := m.client.WritePacket(sent.tip, sent.body, DefaultPacketTimeout)
		require.NoError(t, err)
		err = m.client.Flush()
		require.NoError(t, err)
		var receive errgroup.Group
		var received message
		receive.Go(func() (err error) {
			received.tip, received.body, err = m.server.ReadPacket(nil, DefaultPacketTimeout)
			return err
		})
		require.NoError(t, forward.Wait())
		require.NoError(t, receive.Wait())
		if m.protocolServer == 0 {
			writeAlignTo4 := int(-uint(len(sent.body)) & 3)
			sent.body = append(sent.body, forwardPacketTrailer[writeAlignTo4]...)
		}
		require.Equal(t, sent, received)
	}
}

func TestForwardPacket(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		machine, shutdown, err := newForwardPacketMachine(t)
		require.NoError(t, err)
		machine.run(t)
		shutdown()
	})
}
