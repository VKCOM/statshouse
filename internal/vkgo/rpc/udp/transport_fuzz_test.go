// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package udp

import (
	"log"
	"net/http"
	"os"
	"testing"
)

const corpusDirName = "corpus"

//const crasherDirName = "crashers"

func TestFuzzCorpus(f *testing.T) {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	dir, err := os.ReadDir(corpusDirName)

	if err != nil {
		panic(err)
	}

	for _, file := range dir {
		fuzzFile(corpusDirName + "/" + file.Name())
	}
}

func fuzzFile(filename string) {
	data, err := os.ReadFile(filename)
	if err != nil {
		panic(err)
	}
	log.Println("fuzz", filename)
	FuzzTransport(data)
}
