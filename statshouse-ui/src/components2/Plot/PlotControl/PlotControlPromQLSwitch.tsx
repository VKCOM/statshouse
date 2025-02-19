// Copyright 2025 V Kontakte LLC
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
import { isPromQL } from '@/store2/helpers';
import { getHomePlot, promQLMetric } from '@/url2';
import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';
import { useMetricMeta } from '@/hooks/useMetricMeta';
import { useMetricName } from '@/hooks/useMetricName';
import { useWidgetPlotDataContext } from '@/contexts/useWidgetPlotDataContext';

export type PlotControlPromQLSwitchProps = {
  className?: string;
};

export const PlotControlPromQLSwitch = memo(function PlotControlPromQLSwitch({
  className,
}: PlotControlPromQLSwitchProps) {
  const { plot, setPlot } = useWidgetPlotContext();
  const {
    plotData: { metricName, promQL, whats },
  } = useWidgetPlotDataContext();
  const isPlotPromQL = isPromQL(plot);
  const meta = useMetricMeta(useMetricName(true));

  const onChange = useCallback(() => {
    setPlot((p) => {
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
  }, [meta?.kind, metricName, promQL, setPlot, whats]);

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
