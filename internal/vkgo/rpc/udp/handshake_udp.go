// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package udp

import (
	"crypto/md5"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"net"
	"time"

	"github.com/vkcom/statshouse/internal/vkgo/basictl"
	"github.com/vkcom/statshouse/internal/vkgo/rpc/internal/gen/tlnet"
)

type CryptoKeysUdp struct {
	ReadKey  [32]byte
	WriteKey [32]byte
}

func DeriveCryptoKeysUdp(key string, localPid *tlnet.Pid, remotePid *tlnet.Pid, generation uint32) *CryptoKeysUdp {
	w := writeCryptoInitMsgUdp(key, localPid, remotePid, generation)

	//fmt.Println("init write crypto buf", w)
	var keys CryptoKeysUdp
	w1 := md5.Sum(w[1:])
	w2 := sha1.Sum(w)
	copy(keys.WriteKey[:], w1[:])
	copy(keys.WriteKey[12:], w2[:])

	r := writeCryptoInitMsgUdp(key, remotePid, localPid, generation)
	//fmt.Println("init read crypto buf", r)

	r1 := md5.Sum(r[1:])
	r2 := sha1.Sum(r)
	copy(keys.ReadKey[:], r1[:])
	copy(keys.ReadKey[12:], r2[:])

	return &keys
}

func writeCryptoInitMsgUdp(key string, localPid *tlnet.Pid, remotePid *tlnet.Pid, generation uint32) []byte {
	var message []byte

	message = localPid.Write(message)
	message = append(message, key...)
	message = remotePid.Write(message)
	message = basictl.NatWrite(message, generation)
	return message
}

// We open udp socket but do not read from it.
// Then we write lots of packets there.
// We expect Write to hange when the outgoing UDP buffer is full.
// But in fact we successfully write all packets.
// Then we read packets and notice that most packets were not written.
// This is in contrast with low-level behavior in Linux.
// So there must be a bug in golang.
func TestBackpressureUDP() error {
	address := "127.0.0.1:23337"
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		fmt.Printf("Could not resolve udp addr: %v\n", err)
		return err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		fmt.Printf("Could not listen udp addr: %v\n", err)
		return err
	}
	defer conn.Close()

	conn2, err := net.Dial("udp", address)
	if err != nil {
		fmt.Printf("Could not dial udp addr: %v\n", err)
		return err
	}

	for count := 0; ; count++ {
		ww := make([]byte, 1024)
		binary.LittleEndian.PutUint64(ww, uint64(count))
		n, err := conn2.Write(ww)
		if count == 1000 {
			fmt.Printf("packet 1000\n") // set breakpoint here and step into next Write in debugger
		}
		if err != nil || count == 1001 {
			fmt.Printf("end: %d %d %v\n", n, count, err)
			for {
				rr := make([]byte, 2048)
				n2, err := conn.Read(rr)
				cc := binary.LittleEndian.Uint64(rr)
				fmt.Printf("read: %d %d %v\n", n2, cc, err)
			}
		}
	}
}

func TestBackpressureTCP() error {
	address := "127.0.0.1:23337"
	conn, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Printf("Could not listen udp addr: %v\n", err)
		return err
	}
	var ss net.Conn
	go func() {
		var err3 error
		ss, err3 = conn.Accept()
		if err3 != nil {
			fmt.Printf("Could not accept addr: %v\n", err3)
			return
		}
		defer ss.Close()
		time.Sleep(time.Hour) // prevent GC of ss
	}()
	defer conn.Close()

	conn2, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Printf("Could not dial udp addr: %v\n", err)
		return err
	}

	for count := 0; ; count++ {
		ww := make([]byte, 1024)
		binary.LittleEndian.PutUint64(ww, uint64(count))
		n, err := conn2.Write(ww)
		if count == 100000 {
			fmt.Printf("packet 1000\n") // set breakpoint here and step into next Write in debugger
		}
		if err != nil || count == 100001 {
			fmt.Printf("end: %d %d %v\n", n, count, err)
			for {
				rr := make([]byte, 1024)
				n2, err := ss.Read(rr)
				cc := binary.LittleEndian.Uint64(rr)
				fmt.Printf("read: %d %d %v\n", n2, cc, err)
			}
		}
	}
}
