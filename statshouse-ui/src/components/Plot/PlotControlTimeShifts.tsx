// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useMemo } from 'react';
import cn from 'classnames';

import { selectorParamsPlots, selectorParamsTimeShifts, selectorSetParams, useStore } from '../../store';
import { ToggleButton } from '../UI';
import { produce } from 'immer';
import { getTimeShifts, timeShiftAbbrevExpand, timeShiftDesc } from '../../view/utils2';

export type PlotControlTimeShiftsProps = {
  className?: string;
  customAgg?: number;
};
export function PlotControlTimeShifts({ className }: PlotControlTimeShiftsProps) {
  const plots = useStore(selectorParamsPlots);
  const timeShifts = useStore(selectorParamsTimeShifts);
  const setParams = useStore(selectorSetParams);

  const customAgg = useMemo(() => Math.max(0, ...plots.map((p) => p.customAgg)), [plots]);

  const onChange = useCallback(
    (status: boolean, value?: number) => {
      if (value != null) {
        setParams(
          produce((p) => {
            if (status) {
              p.timeShifts.push(value);
            } else {
              p.timeShifts = p.timeShifts.filter((t) => t !== value);
            }
          })
        );
      }
    },
    [setParams]
  );
  const list = useMemo(() => {
    const shifts = getTimeShifts(customAgg).map(timeShiftAbbrevExpand);
    const l = shifts.map((value) => ({
      value,
      name: timeShiftDesc(value),
      title: timeShiftDesc(value),
      checked: timeShifts.indexOf(value) > -1,
    }));
    const other = timeShifts.filter((t) => shifts.indexOf(t) < 0);
    if (other.length) {
      l.push({ value: other[0], name: '?', title: 'other TimeShift', checked: true });
    }
    return l;
  }, [customAgg, timeShifts]);

  return (
    <div className={cn('btn-group btn-group-sm', className)} role="group">
      {list.map(({ name, value, checked, title }) => (
        <ToggleButton<number>
          key={value}
          className="btn btn-outline-primary"
          checked={checked}
          value={value}
          title={title}
          onChange={onChange}
        >
          {name}
        </ToggleButton>
      ))}
    </div>
  );
}
