// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build embed

package main

import (
	"embed"
	"io/fs"
	"log"
)

var (
	//go:embed build
	staticDir embed.FS
	staticFS  fs.FS
)

func init() {
	var err error
	staticFS, err = fs.Sub(staticDir, "build")
	if err != nil {
		log.Fatalf("invalid embedded static directory: %v", err)
	}
}
