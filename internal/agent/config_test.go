package agent

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUpdateFromRemoteDescription(t *testing.T) {
	// Set defaults
	var master Config
	var f flag.FlagSet
	f.Init("", flag.ContinueOnError)
	master.Bind(&f, DefaultConfig())
	err := f.Parse(nil)
	require.Nil(t, err)
	// Update sample-budget
	copy := master
	err = copy.updateFromRemoteDescription(`
	# comment
	-sample-budget=200000
	`)
	require.Nil(t, err)
	// Verify only sample-budget has changed
	require.Equal(t, copy.SampleBudget, 200000)
	require.NotEqual(t, master, copy)
	copy.SampleBudget = master.SampleBudget
	require.Equal(t, master, copy)
}
