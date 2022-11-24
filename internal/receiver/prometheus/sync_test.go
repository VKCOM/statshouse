// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package prometheus

import (
	"net/url"
	"testing"
)

func Test_parseInstance(t *testing.T) {
	tests := []struct {
		name     string
		uri      string
		wantHost string
		wantPort string
	}{
		{"host and port", "http://localhost:1", "localhost", "1"},
		{"only host and http", "http://localhost/metrics", "localhost", "80"},
		{"only host and https", "https://localhost/metrics", "localhost", "443"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uri, err := url.Parse(tt.uri)
			if err != nil {
				t.Fatalf("bad uri: %s", err.Error())
			}
			gotHost, gotPort := parseInstance(uri)
			if gotHost != tt.wantHost {
				t.Errorf("parseInstance() gotHost = %v, want %v", gotHost, tt.wantHost)
			}
			if gotPort != tt.wantPort {
				t.Errorf("parseInstance() gotPort = %v, want %v", gotPort, tt.wantPort)
			}
		})
	}
}
