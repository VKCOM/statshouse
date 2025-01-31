// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo, useMemo } from 'react';
import { Button, ButtonGroup } from '@grafana/ui';

type Shift = {
  label: string;
  value: number;
  selected: boolean;
};

export const ShiftSelect = memo((props: { shifts: number[]; setShifts: (selectedShifts: number[]) => void }) => {
  const { shifts, setShifts } = props;

  const availableShifts = useMemo(() => {
    const availableShifts: Shift[] = [
      { label: '-24h', value: -86400, selected: false },
      { label: '-48h', value: -172800, selected: false },
      { label: '-1w', value: -604800, selected: false },
      { label: '-2w', value: -1209600, selected: false },
      { label: '-3w', value: -1814400, selected: false },
      { label: '-4w', value: -2419200, selected: false },
      { label: '-1y', value: -31536000, selected: false },
    ];
    availableShifts.map((shift) => {
      shift.selected = shifts.indexOf(shift.value) >= 0;
    });
    return availableShifts;
  }, [shifts]);

  const toggleShift = (shift: Shift) => {
    return () => {
      const values: number[] = [];
      shift.selected = !shift.selected;
      availableShifts.forEach((shift) => {
        if (shift.selected) {
          values.push(shift.value);
        }
      });

      setShifts(values);
    };
  };

  return (
    <ButtonGroup>
      {availableShifts.map((shift) => {
        return (
          <Button
            key={shift.value}
            value={shift.value}
            onClick={toggleShift(shift)}
            variant={shift.selected ? 'primary' : 'secondary'}
          >
            {shift.label}
          </Button>
        );
      })}
    </ButtonGroup>
  );
});

ShiftSelect.displayName = 'ShiftSelect';
