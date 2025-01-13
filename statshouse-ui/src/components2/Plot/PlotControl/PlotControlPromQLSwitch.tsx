// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { memo, useCallback } from 'react';
import { ReactComponent as SVGCode } from 'bootstrap-icons/icons/code.svg';
import { ReactComponent as SVGFilter } from 'bootstrap-icons/icons/filter.svg';
import { Button } from '@/components/UI';
import cn from 'classnames';
import { metricKindToWhat } from '@/view/api';
import type { QueryWhat } from '@/api/enum';
import { type StatsHouseStore, useStatsHouseShallow } from '@/store2';
import { isPromQL } from '@/store2/helpers';
import { getHomePlot, type PlotKey, promQLMetric } from '@/url2';
import { emptyArray } from '@/common/helpers';

export type PlotControlPromQLSwitchProps = {
  plotKey: PlotKey;
  className?: string;
};

export const PlotControlPromQLSwitch = memo(function PlotControlPromQLSwitch({
  plotKey,
  className,
}: PlotControlPromQLSwitchProps) {
  const { isPlotPromQL, meta, setPlot, metricNameData, promQLData, whatsData } = useStatsHouseShallow(
    useCallback(
      ({ params: { plots }, metricMeta, setPlot, plotsData }: StatsHouseStore) => ({
        isPlotPromQL: isPromQL(plots[plotKey]),
        metricName: plots[plotKey]?.metricName ?? '',
        meta: metricMeta[plots[plotKey]?.metricName ?? ''],
        metricNameData: plotsData[plotKey]?.metricName,
        promQLData: plotsData[plotKey]?.promQL ?? '',
        whatsData: plotsData[plotKey]?.whats ?? emptyArray,
        setPlot,
      }),
      [plotKey]
    )
  );
  const onChange = useCallback(() => {
    setPlot(plotKey, (p) => {
      if (isPromQL(p)) {
        const homePlot = getHomePlot();
        if (metricNameData) {
          p.metricName = metricNameData;
          p.what = whatsData?.length ? whatsData.slice() : homePlot.what;
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
        p.promQL = promQLData;
      }
    });
  }, [meta?.kind, metricNameData, plotKey, promQLData, setPlot, whatsData]);

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
});
