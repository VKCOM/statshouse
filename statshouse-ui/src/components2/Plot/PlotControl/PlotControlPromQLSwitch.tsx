// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo, useCallback } from 'react';
import { ReactComponent as SVGCode } from 'bootstrap-icons/icons/code.svg';
import { ReactComponent as SVGFilter } from 'bootstrap-icons/icons/filter.svg';
import { Button } from 'components/UI';
import cn from 'classnames';
import { metricKindToWhat } from 'view/api';
import { type QueryWhat } from 'api/enum';
import { useStatsHouseShallow } from 'store2';
import { isPromQL } from 'store2/helpers';
import { getEmptyPlotData } from 'store2/plotDataStore/getEmptyPlotData';
import { getHomePlot, type PlotKey, promQLMetric } from 'url2';

export type PlotControlPromQLSwitchProps = {
  plotKey: PlotKey;
  className?: string;
};

export function _PlotControlPromQLSwitch({ plotKey, className }: PlotControlPromQLSwitchProps) {
  const { isPlotPromQL, meta, setPlot, plotData } = useStatsHouseShallow(
    ({ params: { plots }, metricMeta, setPlot, plotsData }) => ({
      isPlotPromQL: isPromQL(plots[plotKey]),
      metricName: plots[plotKey]?.metricName ?? '',
      meta: metricMeta[plots[plotKey]?.metricName ?? ''],
      plotData: {
        metricName: plotsData[plotKey]?.metricName,
        promQL: plotsData[plotKey]?.promQL ?? '',
        whats: plotsData[plotKey]?.whats ?? [],
      },
      setPlot,
    })
  );
  const onChange = useCallback(() => {
    const { metricName, promQL, whats } = plotData ?? getEmptyPlotData();
    setPlot(plotKey, (p) => {
      if (isPromQL(p)) {
        const homePlot = getHomePlot();
        if (metricName) {
          p.metricName = metricName;
          p.what = whats?.length ? whats.slice() : homePlot.what;
          p.groupBy = [];
          p.filterIn = {};
          p.filterNotIn = {};
          p.metricUnit = undefined;
          p.promQL = '';
          p.numSeries = homePlot.numSeries;
          p.prometheusCompat = false;
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
  }, [meta?.kind, plotData, plotKey, setPlot]);

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
