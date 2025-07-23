// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package udp

import (
	"fmt"

	"github.com/VKCOM/statshouse/internal/vkgo/rpc/internal/gen/tlnetUdpPacket"
)

type ackRange struct {
	ackFrom, ackTo uint32
	next           *ackRange
}

type AcksToSend struct {
	ackPrefix  uint32
	firstRange *ackRange

	ackSetReused  []uint32
	nackSetReused []tlnetUdpPacket.ResendRange
}

func (a *AcksToSend) AddAckRange(ackFrom, ackTo uint32) {
	if ackFrom <= a.ackPrefix {
		a.ackPrefix = max(a.ackPrefix, ackTo+1)
		for a.firstRange != nil && a.firstRange.ackFrom <= a.ackPrefix {
			a.ackPrefix = max(a.ackPrefix, a.firstRange.ackTo+1)
			a.firstRange = a.firstRange.next
		}
		return
	}

	if a.firstRange == nil {
		a.firstRange = &ackRange{
			ackFrom: ackFrom,
			ackTo:   ackTo,
		}
		return
	}

	var prevRange *ackRange = nil
	tmpRange := a.firstRange

	for {
		if tmpRange == nil || ackTo+1 < tmpRange.ackFrom { // no more ranges or new range is less than tmpRange => need to create new node
			newRange := &ackRange{
				ackFrom: ackFrom,
				ackTo:   ackTo,
				next:    tmpRange,
			}
			if prevRange == nil { // update prev_node's next link
				a.firstRange = newRange
			} else {
				prevRange.next = newRange
			}
			return
		} else if tmpRange.ackTo+1 < ackFrom { // tmpRange is less than new range
			prevRange = tmpRange
			tmpRange = tmpRange.next
		} else { // new range intersects with tmpRange
			if tmpRange.next == nil || ackTo+1 < tmpRange.next.ackFrom { // new range doesn't intersect with next node
				tmpRange.ackFrom = min(tmpRange.ackFrom, ackFrom)
				tmpRange.ackTo = max(tmpRange.ackTo, ackTo)
				return
			} else { // just delete tmpRange node and insert new one further along the cycle
				ackFrom = min(tmpRange.ackFrom, ackFrom)
				if prevRange == nil {
					a.firstRange = tmpRange.next
				} else {
					prevRange.next = tmpRange.next
				}
				tmpRange = tmpRange.next
			}
		}
	}
}

const MaxAckSet = 50

func (a *AcksToSend) HaveHoles() bool {
	return a.firstRange != nil
}

func (a *AcksToSend) BuildAck(enc *tlnetUdpPacket.EncHeader) {
	if a.ackPrefix > 0 {
		enc.SetPacketAckPrefix(a.ackPrefix - 1)
	} else {
		enc.ClearPacketAckPrefix()
	}
	if a.HaveHoles() {
		tmpRange := a.firstRange
		enc.SetPacketAckFrom(tmpRange.ackFrom)
		enc.SetPacketAckTo(tmpRange.ackTo)
		a.ackSetReused = a.ackSetReused[:0]
		for tmpRange.next != nil {
			tmpRange = tmpRange.next
			for seqNum := tmpRange.ackFrom; seqNum <= tmpRange.ackTo && len(a.ackSetReused) < MaxAckSet; seqNum++ {
				a.ackSetReused = append(a.ackSetReused, seqNum)
			}
		}
		if len(a.ackSetReused) > 0 {
			enc.SetPacketAckSet(a.ackSetReused)
		}
	}
}

func (a *AcksToSend) BuildNegativeAck(req *tlnetUdpPacket.ResendRequest) {
	if a.HaveHoles() {
		tmpRange := a.firstRange

		a.nackSetReused = a.nackSetReused[:0]
		a.nackSetReused = append(a.nackSetReused, tlnetUdpPacket.ResendRange{
			PacketNumFrom: a.ackPrefix,
			PacketNumTo:   tmpRange.ackFrom - 1,
		})

		for tmpRange.next != nil && len(a.nackSetReused) < MaxAckSet {
			a.nackSetReused = append(a.nackSetReused, tlnetUdpPacket.ResendRange{
				PacketNumFrom: tmpRange.ackTo + 1,
				PacketNumTo:   tmpRange.next.ackFrom - 1,
			})
			tmpRange = tmpRange.next
		}
		if len(a.nackSetReused) > 0 {
			req.Ranges = a.nackSetReused
		}
	}
}

func (a *AcksToSend) checkInvariantsCommon(onError func(string)) {
	tmpRange := a.firstRange
	if tmpRange != nil {
		if a.ackPrefix >= tmpRange.ackFrom {
			onError(fmt.Sprintf("ackPrefix [0..%d) intersects with the next range [%d..%d]", a.ackPrefix, tmpRange.ackFrom, tmpRange.ackTo))
		}
		for tmpRange.next != nil {
			if tmpRange.ackFrom > tmpRange.next.ackTo {
				// next range doesn't intersect with tmpRange, but lies after this one
				onError(fmt.Sprintf("invalid range order: range [%d..%d] is more than the next range [%d..%d]", tmpRange.ackFrom, tmpRange.ackTo, tmpRange.next.ackFrom, tmpRange.next.ackTo))
			}
			if tmpRange.ackTo+1 >= tmpRange.next.ackFrom {
				onError(fmt.Sprintf("range [%d..%d] intersects with the next range [%d..%d]", tmpRange.ackFrom, tmpRange.ackTo, tmpRange.next.ackFrom, tmpRange.next.ackTo))
			}
			tmpRange = tmpRange.next
		}
	}
}
