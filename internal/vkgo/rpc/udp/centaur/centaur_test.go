// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package centaur

import (
	"crypto/aes"
	"encoding/hex"
	"fmt"
	"log"
	"testing"
)

func TestCentaur(t *testing.T) {
	readKey := [32]byte{1}
	writeKey := [32]byte{2}
	read, err := aes.NewCipher(readKey[:])
	if err != nil {
		log.Panicf("error while generating read crypto keys: %+v", err)
	}
	write, err := aes.NewCipher(writeKey[:])
	if err != nil {
		log.Panicf("error while generating read crypto keys: %+v", err)
	}
	centaurRead, centaurWrite, err := AESCentaur(readKey[:], writeKey[:])
	if err != nil {
		t.Fatalf("error while generating read crypto keys: %+v", err)
	}
	srcWrite := [16]byte{5}
	var dstWrite [16]byte
	var dstCentaurWrite [16]byte
	srcRead := [16]byte{6}
	var dstRead [16]byte
	var dstCentaurRead [16]byte
	write.Encrypt(dstWrite[:], srcWrite[:])
	read.Decrypt(dstRead[:], srcRead[:])
	centaurWrite.Encrypt(dstCentaurWrite[:], srcWrite[:])
	centaurRead.Decrypt(dstCentaurRead[:], srcRead[:])
	dstWriteS := hex.EncodeToString(dstWrite[:])
	dstCentaurWriteS := hex.EncodeToString(dstCentaurWrite[:])
	if debugInternal {
		fmt.Printf("%s %s\n", dstWriteS, dstCentaurWriteS)
	}
	if dstWriteS != dstCentaurWriteS {
		t.Fatal("different dst write")
	}
	if hex.EncodeToString(dstRead[:]) != hex.EncodeToString(dstCentaurRead[:]) {
		t.Fatal("different dst read")
	}
}
