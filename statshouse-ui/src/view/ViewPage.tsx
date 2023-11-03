// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useEffect, useState } from 'react';
import { Dashboard, ErrorMessages, PlotLayout, PlotView } from '../components';
import { setBackgroundColor } from '../common/canvasToImage';
import { Store, useStore } from '../store';
import { now } from './utils';
import { debug } from '../common/debug';
import { shallow } from 'zustand/shallow';
import { usePlotPreview } from '../store/plot/plotPreview';
import { useRectObserver } from '../hooks';

const { setPlotParams, setTimeRange, setBaseRange, setCompact } = useStore.getState();

export type ViewPageProps = {
  embed?: boolean;
  yAxisSize?: number;
};

const selector = ({ params, liveMode, metricsMeta, timeRange, globalNumQueriesPlot }: Store) => ({
  params,
  activePlot: params.plots[params.tabNum],
  liveMode,
  activePlotMeta: metricsMeta[params.plots[params.tabNum]?.metricName ?? ''] ?? undefined,
  globalNumQueriesPlot,
  timeRange,
});
export const ViewPage: React.FC<ViewPageProps> = ({ embed, yAxisSize = 54 }) => {
  const { params, activePlotMeta, activePlot, liveMode, timeRange, globalNumQueriesPlot } = useStore(selector, shallow);
  const plotPreview = usePlotPreview((state) => state.previewList[params.tabNum]);
  const [refPage, setRefPage] = useState<HTMLDivElement | null>(null);
  const [{ width, height }] = useRectObserver(refPage, true);
  useEffect(() => {
    if (embed) {
      window.top?.postMessage({ source: 'statshouse', payload: { width, height } }, '*');
    }
  }, [embed, height, width]);

  useEffect(() => {
    setCompact(!!embed);
  }, [embed]);

  useEffect(() => {
    setBackgroundColor(plotPreview ?? '', 'rgba(255,255,255,1)', 64).then((data) => {
      let link: HTMLLinkElement | null = document.querySelector("link[rel~='icon']");
      if (!link) {
        link = document.createElement('link');
        link.rel = 'icon';
        document.getElementsByTagName('head')[0].appendChild(link);
      }
      link.href = data || '/favicon.ico';
    });
  }, [params.tabNum, plotPreview]);

  const refresh = useCallback(() => {
    if (document.visibilityState === 'visible') {
      setTimeRange(
        (range) => ({
          to: range.absolute ? now() : range.getRangeUrl().to,
          from: range.relativeFrom,
        }),
        true
      );
    }
  }, []);

  useEffect(() => {
    if (liveMode) {
      refresh();
      const refreshSec =
        -timeRange.relativeFrom <= 2 * 3600
          ? 1
          : -timeRange.relativeFrom <= 48 * 3600
          ? 15
          : -timeRange.relativeFrom <= 31 * 24 * 3600
          ? 60
          : 300;
      debug.log('live mode enabled', refreshSec);
      const id = setInterval(refresh, refreshSec * 1000);
      return () => {
        debug.log('live mode disabled');
        clearInterval(id);
      };
    }
  }, [liveMode, refresh, timeRange.relativeFrom]);

  useEffect(() => {
    if (params.tabNum >= 0 && !useStore.getState().dashboardLayoutEdit) {
      window.scrollTo(0, 0);
    }
  }, [params.tabNum]);

  if (params.plots.length === 0) {
    return <ErrorMessages />;
  }
  return (
    <div ref={setRefPage} className="d-flex flex-column flex-md-row">
      <div className={embed ? 'flex-grow-1' : 'flex-grow-1 pt-3 pb-3'}>
        <div className="tab-content position-relative">
          {params.tabNum >= 0 && (
            <div className={`container-xl tab-pane show active ${params.tabNum >= 0 ? '' : 'hidden-dashboard'}`}>
              <PlotLayout
                embed={embed}
                indexPlot={params.tabNum}
                setParams={setPlotParams}
                sel={activePlot}
                meta={activePlotMeta}
                numQueries={globalNumQueriesPlot}
                setBaseRange={setBaseRange}
              >
                {params.plots.map((plot, index) => (
                  <PlotView
                    key={index}
                    indexPlot={index}
                    type={plot.type}
                    compact={!!embed}
                    embed={embed}
                    className={index === params.tabNum ? '' : 'hidden-dashboard'}
                    yAxisSize={yAxisSize}
                    dashboard={false}
                  />
                ))}
              </PlotLayout>
            </div>
          )}
          {params.tabNum < 0 && <Dashboard embed={embed} yAxisSize={yAxisSize} />}
        </div>
      </div>
    </div>
  );
};
