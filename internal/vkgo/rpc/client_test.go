// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewClient(t *testing.T) {
	hooks := ClientHooks{
		InitState: func() any {
			return "init-state"
		},
	}

	var logStr string
	c := NewClient(
		ClientWithLogf(func(format string, args ...interface{}) {
			logStr = fmt.Sprintf("my prefix "+format, args...)
		}),
		ClientWithForceEncryption(true),
		ClientWithCryptoKey("crypto-key"),
		ClientWithHooks(hooks),
		ClientWithConnReadBufSize(123),
		ClientWithConnWriteBufSize(456),
		ClientWithPongTimeout(2*time.Second),
		ClientWithTrustedSubnetGroups([][]string{{"10.32.0.0/11"}}),
	)

	require.Equal(t, "init-state", c.hooks.InitState())
	require.Equal(t, 123, c.connReadBufSize)
	require.Equal(t, 456, c.connWriteBufSize)
	require.Equal(t, 2*time.Second, c.pongTimeout)
	require.Equal(t, "crypto-key", c.cryptoKey)
	require.Equal(t, true, c.forceEncryption)

	c.Logf("123")
	require.Equal(t, "my prefix 123", logStr)

	expectedTrustedSubnetGroups, errs := ParseTrustedSubnets([][]string{{"10.32.0.0/11"}})
	require.Equal(t, expectedTrustedSubnetGroups, c.trustedSubnetGroups)
	require.Nil(t, errs)
}
