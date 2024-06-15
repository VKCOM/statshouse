// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { getNewPlot, type PlotKey, setPlot, useMetricsStore, useUrlStore } from 'store2';
import React, { memo, useCallback, useEffect, useMemo } from 'react';
import { useShallow } from 'zustand/react/shallow';
import { Select, type SelectOptionProps } from 'components';
import cn from 'classnames';
import { QueryWhat } from 'api/enum';
import { metricKindToWhat, whatToWhatDesc } from 'view/api';

export type PlotControlWhatsProps = {
  plotKey: PlotKey;
};

const defaultWhats = getNewPlot().what;

export function _PlotControlWhats({ plotKey }: PlotControlWhatsProps) {
  const { what, metricName } = useUrlStore(
    useShallow((s) => ({
      what: s.params.plots[plotKey]?.what ?? defaultWhats,
      metricName: s.params.plots[plotKey]?.metricName ?? '',
    }))
  );

  const meta = useMetricsStore((s) => s.meta[metricName]);
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
    [meta?.kind, plotKey]
  );

  useEffect(() => {
    const whats = metricKindToWhat(meta?.kind);
    if (what.some((qw) => whats.indexOf(qw) === -1)) {
      setPlot(plotKey, (p) => {
        p.what = [whats[0] as QueryWhat];
      });
    }
  }, [meta?.kind, plotKey, what]);
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
