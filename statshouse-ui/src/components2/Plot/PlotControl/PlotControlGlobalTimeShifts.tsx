// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { memo, useCallback, useMemo } from 'react';
import cn from 'classnames';
import { ToggleButton } from '@/components/UI';
import { getTimeShifts, timeShiftAbbrevExpand, timeShiftDesc } from '@/view/utils2';
import { setParams } from '@/store2/methods';
import { StatsHouseStore, useStatsHouse, useStatsHouseShallow } from '@/store2';
import { selectorOrderPlot } from '@/store2/selectors';

export type PlotControlGlobalTimeShiftsProps = {
  className?: string;
};

const selectorStore = ({ params: { timeShifts, plots } }: StatsHouseStore) => ({
  timeShifts,
  plots,
});

export const PlotControlGlobalTimeShifts = memo(function PlotControlGlobalTimeShifts({
  className,
}: PlotControlGlobalTimeShiftsProps) {
  const { timeShifts, plots } = useStatsHouseShallow(selectorStore);
  const orderPlot = useStatsHouse(selectorOrderPlot);

  const maxCustomAgg = useMemo(
    () => Math.max(0, ...orderPlot.map((pK) => plots[pK]?.customAgg ?? 0)),
    [orderPlot, plots]
  );

  const onChange = useCallback((status: boolean, value?: number) => {
    if (value == null) {
      return;
    }
    setParams((p) => {
      if (status) {
        p.timeShifts.push(value);
      } else {
        p.timeShifts = p.timeShifts.filter((t) => t !== value);
      }
    });
  }, []);

  const list = useMemo(() => {
    const shifts = getTimeShifts(maxCustomAgg).map(timeShiftAbbrevExpand);
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
  }, [maxCustomAgg, timeShifts]);

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
});
