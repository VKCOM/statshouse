// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo, useCallback, useEffect, useMemo } from 'react';
import { Select, type SelectOptionProps } from 'components/Select';
import cn from 'classnames';
import { type QueryWhat } from 'api/enum';
import { metricKindToWhat } from 'view/api';
import { getNewMetric, type PlotKey } from 'url2';
import { useStatsHouseShallow } from 'store2';
import { whatToWhatDesc } from 'view/whatToWhatDesc';

export type PlotControlWhatsProps = {
  plotKey: PlotKey;
};

const defaultWhats = getNewMetric().what;

export function _PlotControlWhats({ plotKey }: PlotControlWhatsProps) {
  const { what, meta, setPlot } = useStatsHouseShallow((s) => ({
    what: s.params.plots[plotKey]?.what ?? defaultWhats,
    meta: s.metricMeta[s.params.plots[plotKey]?.metricName ?? ''],
    setPlot: s.setPlot,
  }));

  const options = useMemo(() => {
    const whats: SelectOptionProps[] = metricKindToWhat(meta?.kind).map((w) => ({
      value: w,
      name: whatToWhatDesc(w),
      disabled: w === '-',
      splitter: w === '-',
    }));
    return whats;
  }, [meta?.kind]);
  const onChange = useCallback(
    (nextValues?: string | string[]) => {
      const whatValue = Array.isArray(nextValues) ? nextValues : nextValues ? [nextValues] : [];
      if (!whatValue.length) {
        const whats = metricKindToWhat(meta?.kind);
        whatValue.push(whats[0]);
      }
      setPlot(plotKey, (s) => {
        s.what = whatValue as QueryWhat[];
      });
    },
    [meta?.kind, plotKey, setPlot]
  );

  useEffect(() => {
    const whats = metricKindToWhat(meta?.kind);
    if (what.some((qw) => whats.indexOf(qw) === -1)) {
      setPlot(plotKey, (p) => {
        p.what = [whats[0] as QueryWhat];
      });
    }
  }, [meta?.kind, plotKey, setPlot, what]);
  return (
    <Select
      value={what}
      onChange={onChange}
      options={options}
      multiple
      onceSelectByClick
      className={cn('sh-select form-control')}
      classNameList="dropdown-menu"
    />
  );
}

export const PlotControlWhats = memo(_PlotControlWhats);
