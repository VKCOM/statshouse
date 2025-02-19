// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { memo, useCallback, useEffect, useMemo } from 'react';
import { Select, type SelectOptionProps } from '@/components/Select';
import cn from 'classnames';
import type { QueryWhat } from '@/api/enum';
import { metricKindToWhat } from '@/view/api';
import { whatToWhatDesc } from '@/view/whatToWhatDesc';
import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';
import { useMetricMeta } from '@/hooks/useMetricMeta';
import { useMetricName } from '@/hooks/useMetricName';

export const PlotControlWhats = memo(function PlotControlWhats() {
  const {
    plot: { what },
    setPlot,
  } = useWidgetPlotContext();
  const meta = useMetricMeta(useMetricName(true));

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
      setPlot((s) => {
        s.what = whatValue as QueryWhat[];
      });
    },
    [meta?.kind, setPlot]
  );

  useEffect(() => {
    const whats = metricKindToWhat(meta?.kind);
    if (what.some((qw) => whats.indexOf(qw) === -1)) {
      setPlot((p) => {
        p.what = [whats[0] as QueryWhat];
      });
    }
  }, [meta?.kind, setPlot, what]);
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
});
