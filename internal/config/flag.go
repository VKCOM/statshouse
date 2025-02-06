package config

import (
	"flag"
	"fmt"
)

type stringSlice struct {
	p      *[]string
	wasSet bool
}

func StringSliceVar(p *[]string, name string, value string, usage string) {
	stringSliceVar(flag.CommandLine, p, name, value, usage)
}

func stringSliceVar(f *flag.FlagSet, p *[]string, name string, value string, usage string) {
	*p = parseCSV(value)
	f.Var(&stringSlice{p: p}, name, usage)
}

func (s *stringSlice) Set(v string) error {
	if s.wasSet {
		*s.p = append(*s.p, parseCSV(v)...)
	} else {
		*s.p = parseCSV(v)
		s.wasSet = true
	}
	return nil
}

func (s *stringSlice) String() string {
	if len(*s.p) == 0 {
		return "[]"
	}
	return fmt.Sprint(*s.p)
}

func parseCSV(s string) []string {
	var res []string
	for i := 0; i < len(s); {
		j := i
		for ; j < len(s) && s[j] != ',' && s[j] != ';'; j++ {
			// pass
		}
		res = append(res, s[i:j])
		i = j + 1
	}
	return res
}
