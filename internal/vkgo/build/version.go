// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package build

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path"
	"runtime"
	"strconv"
)

var (
	// Build* заполняются при сборке go build -ldflags
	time                string
	machine             string
	commit              string
	commitTimestamp     string
	version             string
	number              string
	trustedSubnetGroups string

	appName              string
	commitTimestampInt64 int64
	trustedSubnetGroupsS [][]string
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

// UNIX timestampt seconds, so stable in any TZ
func CommitTimestamp() int64 {
	return commitTimestampInt64
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
	commitTimestampInt64, _ = strconv.ParseInt(commitTimestamp, 10, 64)
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
		_, _ = fmt.Fprintf(os.Stderr, "%s\n", Info())
		os.Exit(0)
	}
	if help {
		_, _ = fmt.Fprintf(os.Stderr, "Usage of %s:\n", AppName())
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
	err := json.Unmarshal([]byte(trustedSubnetGroups), &trustedSubnetGroupsS)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to parse TrustedSubnetGroups %q: %v\n", trustedSubnetGroups, err)
		os.Exit(1)
	}
}

func TrustedSubnetGroups() [][]string {
	return trustedSubnetGroupsS
}
