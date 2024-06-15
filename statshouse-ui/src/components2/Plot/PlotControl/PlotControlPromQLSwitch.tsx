// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo, useCallback } from 'react';
import { ReactComponent as SVGCode } from 'bootstrap-icons/icons/code.svg';
import { ReactComponent as SVGFilter } from 'bootstrap-icons/icons/filter.svg';
import { Button } from 'components';
import cn from 'classnames';
import {
  getEmptyPlotData,
  getHomePlot,
  isPromQL,
  type PlotKey,
  promQLMetric,
  setPlot,
  useMetricsStore,
  usePlotsDataStore,
  useUrlStore,
} from 'store2';
import { metricKindToWhat } from 'view/api';
import { type QueryWhat } from 'api/enum';
import { useShallow } from 'zustand/react/shallow';

export type PlotControlPromQLSwitchProps = {
  plotKey: PlotKey;
  className?: string;
};

export function _PlotControlPromQLSwitch({ plotKey, className }: PlotControlPromQLSwitchProps) {
  const { isPlotPromQL, metricName } = useUrlStore(
    useShallow((s) => ({
      isPlotPromQL: isPromQL(s.params.plots[plotKey]),
      metricName: s.params.plots[plotKey]?.metricName ?? '',
    }))
  );
  const meta = useMetricsStore((s) => s.meta[metricName]);
  const onChange = useCallback(() => {
    const { nameMetric, promQL, whats } = usePlotsDataStore.getState().plotsData[plotKey] ?? getEmptyPlotData();
    setPlot(plotKey, (p) => {
      if (isPromQL(p)) {
        const homePlot = getHomePlot();
        if (nameMetric) {
          p.metricName = nameMetric;
          p.what = whats?.length ? whats.slice() : homePlot.what;
          p.groupBy = [];
          p.filterIn = {};
          p.filterNotIn = {};
          p.metricUnit = undefined;
          p.promQL = '';
          p.numSeries = homePlot.numSeries;
        } else {
          return { ...homePlot, id: p.id, maxHost: p.maxHost };
        }
      } else {
        const whats = metricKindToWhat(meta?.kind);
        p.metricName = promQLMetric;
        p.what = [whats[0] as QueryWhat];
        p.groupBy = [];
        p.filterIn = {};
        p.filterNotIn = {};
        p.promQL = promQL;
      }
    });
  }, [meta?.kind, plotKey]);

  return (
    <Button
      type="button"
      title={isPlotPromQL ? 'filter' : 'PromQL'}
      className={cn('btn btn-outline-primary', className)}
      onClick={onChange}
    >
      {isPlotPromQL ? <SVGFilter /> : <SVGCode />}
    </Button>
  );
}

export const PlotControlPromQLSwitch = memo(_PlotControlPromQLSwitch);
