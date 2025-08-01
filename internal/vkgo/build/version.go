// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package build

import (
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"

	"github.com/VKCOM/statshouse/internal/vkgo/rpc"
)

var (
	// Build* заполняются при сборке go build -ldflags
	time                string
	machine             string
	commit              string
	commitTag           uint32
	commitTimestamp     string
	version             string
	number              string
	trustedSubnetGroups string

	appName               string
	commitTimestampUint32 uint32
	trustedSubnetGroupsS  [][]string
)

func Time() string {
	if time == "" {
		return "?"
	}
	return time
}

func Machine() string {
	if machine == "" {
		return "?"
	}
	return machine
}

func Commit() string {
	if commit == "" {
		return "?"
	}
	return commit
}

// appropriate for inserting into statshouse raw key
func CommitTag() uint32 {
	return commitTag
}

// UNIX timestampt seconds, so stable in any TZ
func CommitTimestamp() uint32 {
	return commitTimestampUint32
}

func Version() string {
	if version == "" {
		return "?"
	}
	return version
}

func Number() string {
	if number == "" {
		return "?"
	}
	return number
}

func Info() string {
	return fmt.Sprintf("%s compiled at %s by %s after %s on %s build %s", appName, Time(), runtime.Version(), Version(), Machine(), Number())
}

func init() {
	appName = path.Base(os.Args[0])
	ts, _ := strconv.ParseUint(commitTimestamp, 10, 32)
	commitTimestampUint32 = uint32(ts)
	if commitTagRaw, _ := hex.DecodeString(commit); len(commitTagRaw) >= 4 {
		commitTag = binary.BigEndian.Uint32(commitTagRaw[:])
	}
	parseTrustedSubnetGroups()
}

func AppName() string { // TODO - remember during build
	return appName
}

func FlagParseShowVersionHelpWithTail() {
	help := false
	version := false
	flag.BoolVar(&help, `h`, false, `show this help`)
	flag.BoolVar(&help, `help`, false, `show this help`)
	flag.BoolVar(&version, `v`, false, `show version`)
	flag.BoolVar(&version, `version`, false, `show version`)

	flag.Parse()

	if version {
		_, _ = fmt.Printf("%s\n", Info())
		os.Exit(0)
	}
	if help {
		_, _ = fmt.Printf("Usage of %s:\n", AppName())
		flag.PrintDefaults()
		os.Exit(0)
	}
}

// Fatals if additional parameters passed. Protection against 'kittenhosue ch-addr=x -c=y' when dash is forgotten
func FlagParseShowVersionHelp() {
	FlagParseShowVersionHelpWithTail()
	if len(flag.Args()) != 0 {
		_, _ = fmt.Fprintf(os.Stderr, "Unexpected command line argument - %q, check command line for typos\n", flag.Args()[0])
		os.Exit(1)
	}
}

func parseTrustedSubnetGroups() {
	if len(trustedSubnetGroups) == 0 {
		return
	}
	for _, group := range strings.Split(trustedSubnetGroups, ";") {
		var s []string
		for _, addr := range strings.Split(group, ",") {
			t := strings.TrimSpace(addr)
			if len(t) != 0 {
				s = append(s, t)
			}
		}
		if len(s) != 0 {
			trustedSubnetGroupsS = append(trustedSubnetGroupsS, s)
		}
	}
	_, errs := rpc.ParseTrustedSubnets(trustedSubnetGroupsS)
	if len(errs) != 0 {
		for _, err := range errs {
			log.Printf("failed to parse trusted subnet: %q", err)
		}
		os.Exit(1)
	}
}

func TrustedSubnetGroups() [][]string {
	return trustedSubnetGroupsS
}
