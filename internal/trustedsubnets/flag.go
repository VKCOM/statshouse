// Copyright 2026 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package trustedsubnets

import (
	"flag"
	"fmt"
	"strings"

	"github.com/VKCOM/statshouse/internal/vkgo/rpc"
)

type Flag struct {
	groups [][]string
	set    bool
}

func (f *Flag) String() string { return "" }

func (f *Flag) Set(s string) error {
	f.set = true
	if s == "" {
		f.groups = nil
		return nil
	}
	groups, err := ParseGroups(s)
	if err != nil {
		return err
	}
	f.groups = groups
	return nil
}

func (f *Flag) Type() string { return "trusted-subnet-groups" }

func (f *Flag) GetOrDefault(defaultGroups [][]string) [][]string {
	if f.set {
		return f.groups
	}
	return defaultGroups
}

func (f *Flag) Bind(fs *flag.FlagSet) {
	fs.Var(f, "trusted-subnet-groups", "trusted subnet groups; format: group1,group1b;group2 (group - comma-separated CIDR list)")
}

func ParseGroups(src string) ([][]string, error) {
	if len(src) == 0 {
		return nil, nil
	}
	var groups [][]string
	for _, group := range strings.Split(src, ";") {
		var s []string
		for _, addr := range strings.Split(group, ",") {
			t := strings.TrimSpace(addr)
			if len(t) != 0 {
				s = append(s, t)
			}
		}
		if len(s) != 0 {
			groups = append(groups, s)
		}
	}
	_, errs := rpc.ParseTrustedSubnets(groups)
	if len(errs) != 0 {
		var b strings.Builder
		for i, err := range errs {
			if i > 0 {
				b.WriteString("; ")
			}
			b.WriteString(err.Error())
		}
		return nil, fmt.Errorf("%s", b.String())
	}
	return groups, nil
}
