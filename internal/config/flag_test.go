package config

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStringSliceVarDefaults(t *testing.T) {
	var arg1, arg2 []string
	f := flag.NewFlagSet("", flag.ContinueOnError)
	StringSliceVar(f, &arg1, "arg1", "foo,bar", "usage")
	StringSliceVar(f, &arg2, "arg2", "bar;foo", "usage")
	require.NoError(t, f.Parse(nil))
	require.EqualValues(t, []string{"foo", "bar"}, arg1)
	require.EqualValues(t, []string{"bar", "foo"}, arg2)
}

func TestStringSliceVar(t *testing.T) {
	var arg []string
	f := flag.NewFlagSet("", flag.ContinueOnError)
	StringSliceVar(f, &arg, "arg1", "foo,bar", "usage")
	require.NoError(t, f.Parse([]string{"--arg1=1,2", "--arg1=3,4"}))
	require.EqualValues(t, []string{"1", "2", "3", "4"}, arg)
}
