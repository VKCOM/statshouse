// Copyright 2025 V Kontakte LLC
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
	"os"
	"path"
	"runtime"
	"strconv"
	"time"
)

var (
	// Build* заполняются при сборке go build -ldflags
	buildTimestamp  string
	machine         string
	commit          string
	commitTag       uint32
	commitTimestamp string
	version         string
	number          string
	branchName      string
	name            string

	info string // combination of above

	appName               string
	commitTimestampUint32 uint32
	commitTimeFormatted   string

	buildTimeFormatted   string
	buildTimestampUint32 uint32

	trustedSubnetGroups string
)

func Time() string {
	return buildTimeFormatted
}

func Timestamp() uint32 {
	return buildTimestampUint32
}

func Machine() string {
	return machine
}

func Commit() string {
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

func CommitTime() string {
	return commitTimeFormatted
}

func Version() string {
	return version
}

func Number() string {
	return number
}

func Name() string {
	return name
}

func BranchName() string {
	return branchName
}

func Info() string {
	return info
}

func init() {
	appName = path.Base(os.Args[0])
	ts, _ := strconv.ParseUint(commitTimestamp, 10, 32)
	commitTimestampUint32 = uint32(ts)
	commitTimeFormatted = formatTime(ts)

	if commitTagRaw, _ := hex.DecodeString(commit); len(commitTagRaw) >= 4 {
		commitTag = binary.BigEndian.Uint32(commitTagRaw[:])
	}

	if buildTimestamp != "" {
		ts, _ = strconv.ParseUint(buildTimestamp, 10, 32)
		buildTimestampUint32 = uint32(ts)
		buildTimeFormatted = formatTime(ts)
	}

	info = fmt.Sprintf("%s compiled at %s by %s after %s on %s build %s", appName, buildTimeFormatted, runtime.Version(), version, machine, number)
}

func formatTime(t uint64) string {
	return time.Unix(int64(t), 0).Format("2006-01-02T15:04:05-0700")
}

func AppName() string { // TODO - remember during build?
	return appName
}

func FlagParseShowVersionHelpWithTail(set *flag.FlagSet, args []string) {
	help := false
	ver := false
	set.BoolVar(&help, "h", false, "show this help")
	set.BoolVar(&help, "help", false, "show this help")
	set.BoolVar(&ver, "v", false, "show version")
	set.BoolVar(&ver, "version", false, "show version")

	err := set.Parse(args)
	if err != nil {
		os.Exit(2) // enforce ExitOnError policy
	}

	if ver {
		_, _ = fmt.Fprintf(os.Stderr, "%s\n", Info())
		os.Exit(0)
	}
	if help {
		if set.Usage != nil {
			set.Usage()
		} else {
			_, _ = fmt.Fprintf(os.Stderr, "Usage of %s:\n", set.Name())
			set.PrintDefaults()
		}
		os.Exit(0)
	}
}

func FlagSetParseShowVersionHelp(set *flag.FlagSet, args []string) {
	FlagParseShowVersionHelpWithTail(set, args)
	if len(set.Args()) != 0 {
		_, _ = fmt.Fprintf(os.Stderr, "Unexpected command line argument - %q, check command line for typos\n", set.Args()[0])
		os.Exit(1)
	}
}

// Fatals if additional parameters passed. Protection against 'kittenhosue ch-addr=x -c=y' when dash is forgotten
func FlagParseShowVersionHelp() {
	FlagSetParseShowVersionHelp(flag.CommandLine, os.Args[1:])
}

// use ';' or ',' to override non-empty list with an empty list
func TrustedSubnetGroups(_default string) string {
	if trustedSubnetGroups != "" {
		return trustedSubnetGroups
	}
	return _default
}
