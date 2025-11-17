// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package centaur

import (
	"crypto/aes"
	"crypto/cipher"
	"fmt"
	"reflect"
	"unsafe"
)

// unsafely combine 2 standard AES cipher.Block(s) to use 2x less memory
// each cipher.Block contains context for both encoding and decoding,
// but we have a different key for each direction, so only encode or decode with each cipher.Block

const debugInternal = false // set to true to help with debugging new internal implementation

type eface struct {
	typ, val unsafe.Pointer
}

type aesCipher struct {
	l   uint8 // only this length of the enc and dec array is actually used
	enc [28 + 32]uint32
	dec [28 + 32]uint32
}

func accessaesCipher(c cipher.Block) *aesCipher {
	iptr := (*eface)(unsafe.Pointer(&c))

	return (*aesCipher)(iptr.val)
}

type blockExpanded struct {
	rounds int
	// Round keys, where only the first (rounds + 1) × (128 ÷ 32) words are used.
	enc [60]uint32
	dec [60]uint32
}

func accessBlockExpanded(c cipher.Block) *blockExpanded {
	iptr := (*eface)(unsafe.Pointer(&c))

	return (*blockExpanded)(iptr.val)
}

func AESCentaur(readKey []byte, writeKey []byte) (read cipher.Block, write cipher.Block, err error) {
	read, err = aes.NewCipher(readKey)
	if err != nil {
		return nil, nil, fmt.Errorf("error while generating read crypto keys: %w", err)
	}
	write, err = aes.NewCipher(writeKey)
	if err != nil {
		return nil, nil, fmt.Errorf("error while generating read crypto keys: %w", err)
	}
	t := reflect.TypeOf(read)
	if debugInternal {
		fmt.Printf("%s\n", t.String())
	}
	switch t.String() {
	case "*aes.aesCipherGCM": // go 1.23.9
		readInternal := accessaesCipher(read)
		writeInternal := accessaesCipher(write)
		if debugInternal {
			fmt.Printf("%d %v %v\n", readInternal.l, readInternal.dec[:], readInternal.enc[:])
			fmt.Printf("%d %v %v\n", writeInternal.l, writeInternal.dec[:], writeInternal.enc[:])
		}
		(*readInternal).enc = (*writeInternal).enc
		return read, read, nil
	case "*aes.Block": // go 1.24.0
		readInternal := accessBlockExpanded(read)
		writeInternal := accessBlockExpanded(write)
		if debugInternal {
			fmt.Printf("%d %v %v\n", readInternal.rounds, readInternal.dec[:], readInternal.enc[:])
			fmt.Printf("%d %v %v\n", writeInternal.rounds, writeInternal.dec[:], writeInternal.enc[:])
		}
		(*readInternal).enc = (*writeInternal).enc
		return read, read, nil
	}
	// unknown implementation - use 2x memory, but do not fail
	return read, write, nil
}
