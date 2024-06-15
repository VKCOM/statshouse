// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { type ChangeEvent, memo, useCallback } from 'react';

import { formatInputDate, formatInputTime, maxTimeRange, now, parseInputDate, parseInputTime } from 'view/utils';
import { ReactComponent as SVGLockClock } from 'assets/svg/LockClock.svg';
import { ReactComponent as SVGUnlockClock } from 'assets/svg/UnlockClock.svg';
import cn from 'classnames';
import { constToTime, readTimeRange, setTimeRange, useUrlStore } from 'store2';
import { ToggleButton } from 'components';
import { produce } from 'immer';
import { TIME_RANGE_KEYS_TO } from 'api/enum';

export type PlotControlToProps = {
  className?: string;
  classNameInput?: string;
};

export const _PlotControlTo: React.FC<PlotControlToProps> = ({ className, classNameInput }) => {
  const timeRange = useUrlStore((s) => s.params.timeRange);

  const onRelativeToChange = useCallback((status: boolean) => {
    setTimeRange(
      produce((t) => {
        if (status) {
          return readTimeRange(
            t.from,
            Object.values(TIME_RANGE_KEYS_TO).find((key) => Math.abs(t.to - constToTime(t.now, key)) < 60) ??
              TIME_RANGE_KEYS_TO.Now
          );
        } else {
          return {
            ...t,
            urlTo: t.to,
          };
        }
      })
    );
  }, []);

  const onToDateChange = useCallback(
    (e: ChangeEvent<HTMLInputElement>) => {
      const v = e.target.value;
      const [y, m, d] = parseInputDate(v);
      if (v === '' || y < 1900) {
        return;
      }
      const nextTime = new Date(timeRange.to * 1000);
      nextTime.setFullYear(y, m, d);
      setTimeRange(
        produce((t) => {
          t.urlTo = t.to = Math.floor(+nextTime / 1000);
        })
      );
    },
    [timeRange]
  );

  const onToTimeChange = useCallback(
    (e: ChangeEvent<HTMLInputElement>) => {
      const v = e.target.value;
      const [h, m, sec] = v !== '' ? parseInputTime(v) : [0, 0, 0];
      const nextTime = new Date(timeRange.to * 1000);
      nextTime.setHours(h, m, sec);
      setTimeRange(
        produce((t) => {
          t.urlTo = t.to = Math.floor(+nextTime / 1000);
        })
      );
    },
    [timeRange]
  );

  return (
    <div className={cn('input-group flex-nowrap', className)}>
      <input
        type="date"
        className={`form-control form-control-safari-fix ${classNameInput}`}
        disabled={!timeRange.absolute}
        max={formatInputDate(Math.min(timeRange.to + timeRange.from + maxTimeRange, now()))}
        value={formatInputDate(timeRange.to)}
        onChange={onToDateChange}
      />
      <input
        type="time"
        className={`form-control form-control-safari-fix ${classNameInput}`}
        disabled={!timeRange.absolute}
        step="1"
        value={formatInputTime(timeRange.to)}
        onChange={onToTimeChange}
      />
      <ToggleButton
        className="btn btn-outline-primary"
        title="Use relative date/time in URL"
        checked={!timeRange.absolute}
        onChange={onRelativeToChange}
      >
        {!timeRange.absolute ? <SVGLockClock /> : <SVGUnlockClock />}
      </ToggleButton>
    </div>
  );
};

export const PlotControlTo = memo(_PlotControlTo);
