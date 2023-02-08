// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { ChangeEvent, Fragment, useCallback, useMemo } from 'react';
import cn from 'classnames';

import * as utils from '../../view/utils';
import { selectorParamsPlots, selectorParamsTimeShifts, selectorSetParams, useStore } from '../../store';

export type PlotControlTimeShiftsProps = {
  className?: string;
  customAgg?: number;
};
export function PlotControlTimeShifts({ className }: PlotControlTimeShiftsProps) {
  const plots = useStore(selectorParamsPlots);
  const timeShifts = useStore(selectorParamsTimeShifts);
  const setParams = useStore(selectorSetParams);

  const customAgg = useMemo(() => Math.max(0, ...plots.map((p) => p.customAgg)), [plots]);

  const onTimeShiftChange = useCallback(
    (e: ChangeEvent<HTMLInputElement>) => {
      const ts = parseInt(e.target.value);
      let shifts = timeShifts.filter((v) => v !== ts);
      if (e.target.checked) {
        shifts.push(ts);
      }
      setParams((p) => ({
        ...p,
        timeShifts: shifts,
      }));
    },
    [setParams, timeShifts]
  );

  return (
    <div className={cn('btn-group btn-group-sm', className)} role="group">
      {utils.getTimeShifts(customAgg).map((ts) => {
        const dt = utils.timeShiftAbbrevExpand(ts);
        return (
          <Fragment key={ts}>
            <input
              type="checkbox"
              className="btn-check"
              id={`compare${ts}`}
              autoComplete="off"
              value={dt}
              checked={timeShifts.indexOf(dt) !== -1}
              onChange={onTimeShiftChange}
            />
            <label className="btn btn-outline-primary" htmlFor={`compare${ts}`}>
              {utils.timeShiftDesc(utils.timeShiftAbbrevExpand(ts))}
            </label>
          </Fragment>
        );
      })}
    </div>
  );
}
