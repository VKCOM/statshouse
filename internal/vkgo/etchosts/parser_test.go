// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package etchosts

import (
	"testing"

	"github.com/stretchr/testify/require"
)

/*
BenchmarkReadHosts-12       63252         15850 ns/op     2414 B/op       30 allocs/op
BenchmarkResolve-12      32651385         37.94 ns/op        0 B/op        0 allocs/op
*/

func BenchmarkReadHosts(b *testing.B) {
	b.ReportAllocs()

	hostsFile = "testdata/hosts"

	var hostnameToIP map[string]string
	for i := 0; i < b.N; i++ {
		hostnameToIP = readHosts()
	}
	_ = hostnameToIP
}

func BenchmarkResolve(b *testing.B) {
	b.ReportAllocs()

	hostsFile = "testdata/hosts"

	var ip string
	for i := 0; i < b.N; i++ {
		ip = Resolve("eva612345")
	}
	_ = ip
}

func TestReadHosts(t *testing.T) {
	hostsFile = "testdata/hosts"

	hostnameToIP := readHosts()
	require.Equal(t, expectedHostnameToIP, hostnameToIP)
}

var expectedHostnameToIP = map[string]string{
	"eva612345":              "192.168.0.200",
	"cvt667890.if1":          "120.191.170.23",
	"fast612345":             "192.168.0.200",
	"fast612345.example.com": "192.168.0.200",
	"fast667890.if1":         "120.191.170.23",
	"gzn912345":              "200.4.100.5",
	"old812345":              "190.0.123.168",
	"old812345.example.com":  "190.0.123.168",
	"old912345":              "200.4.100.5",
	"sdn812345":              "190.0.123.168",
}
