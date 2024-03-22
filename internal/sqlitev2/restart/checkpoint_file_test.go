package restart

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func Test_encode(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		commitOffset := rapid.Int64().Draw(t, "commit_offset")
		actualCommitOffset, err := decode(encode(commitOffset))
		require.NoError(t, err)
		require.Equal(t, commitOffset, actualCommitOffset)
	})
}
