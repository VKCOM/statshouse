package api

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCache2SeriesList(t *testing.T) {
	s := [100]cache2Series{}
	l := newCache2SeriesList()
	require.Equal(t, 0, l.len())
	for i := 0; i < len(s); i++ {
		l.add(&s[i])
		require.Equal(t, i+1, l.len())
	}
	for i := 0; i < len(s); i++ {
		l.remove(&s[i])
		require.Equal(t, len(s)-i-1, l.len())
	}
}
