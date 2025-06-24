// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package udp

import (
	"testing"

	"pgregory.net/rand"
)

func TestAcksRandom(t *testing.T) {
	maxAckInterval := uint32(100)

	acksDS := AcksToSend{
		ackPrefix: rand.Uint32n(maxAckInterval), // to have chance test different ack prefixes and their changes
	}
	acksSet := make(map[uint32]struct{})
	for ackNum := uint32(0); ackNum < acksDS.ackPrefix; ackNum++ {
		acksSet[ackNum] = struct{}{}
	}

	// N = maxAckInterval
	// O(N) * O(N^2) = O(N^3)
	for i := 0; i < int(3*maxAckInterval); i++ {
		ackFrom := rand.Uint32n(maxAckInterval * maxAckInterval)
		ackTo := ackFrom + rand.Uint32n(maxAckInterval)

		// O(N)
		acksDS.AddAckRange(ackFrom, ackTo)
		acksDS.checkInvariantsTesting(t)

		for ackNum := ackFrom; ackNum <= ackTo; ackNum++ {
			acksSet[ackNum] = struct{}{}
		}
		// O(|acks|) = (N^2)
		checkEqual(acksDS, acksSet, t)
	}
}

func checkEqual(ds AcksToSend, set map[uint32]struct{}, t *testing.T) {
	// check that set contains all acks from ds
	for ackNum := uint32(0); ackNum < ds.ackPrefix; ackNum++ {
		if _, exists := set[ackNum]; !exists {
			t.Fatalf("AcksToSend.ackPrefix(%d) is invalid: real ack set doesn't contain %d", ds.ackPrefix, ackNum)
		}
	}
	tmpRange := ds.firstRange
	for tmpRange != nil {
		for ackNum := tmpRange.ackFrom; ackNum <= tmpRange.ackTo; ackNum++ {
			if _, exists := set[ackNum]; !exists {
				t.Fatalf("AcksToSend range [%d..%d] is invalid: real ack set doesn't contain %d", tmpRange.ackFrom, tmpRange.ackTo, ackNum)
			}
		}
		tmpRange = tmpRange.next
	}

	// check that ds contains all acks from set
checkAcksLoop:
	for ackNum := range set {
		if ackNum < ds.ackPrefix {
			continue
		}
		var prevRange *ackRange = nil
		tmpRange = ds.firstRange
		for tmpRange != nil {
			if ackNum < tmpRange.ackFrom {
				if prevRange == nil {
					t.Fatalf("AcksToSend doesn't contain ack %d from the real ack set. Nearest ranges are [0..%d) and [%d..%d]", ackNum, ds.ackPrefix, tmpRange.ackFrom, tmpRange.ackTo)
				} else {
					t.Fatalf("AcksToSend doesn't contain ack %d from the real ack set. Nearest ranges are [%d..%d] and [%d..%d]", ackNum, prevRange.ackFrom, prevRange.ackTo, tmpRange.ackFrom, tmpRange.ackTo)
				}
			}
			if tmpRange.ackFrom <= ackNum && ackNum <= tmpRange.ackTo {
				continue checkAcksLoop
			}
			prevRange = tmpRange
			tmpRange = tmpRange.next
		}
		t.Fatalf("AcksToSend doesn't contain ack %d from the real ack set. Last range is [%d..%d]", ackNum, prevRange.ackFrom, prevRange.ackTo)
	}
}

func (a *AcksToSend) checkInvariantsTesting(t *testing.T) {
	a.checkInvariantsCommon(
		func(err string) { t.Fatal(err) },
	)
}
