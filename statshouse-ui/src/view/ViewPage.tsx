// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useEffect, useMemo } from 'react';
import { useSearchParams } from 'react-router-dom';
import { Dashboard, PlotLayout, PlotView } from '../components';
import { setBackgroundColor } from '../common/canvasToImage';
import {
  selectorGlobalNumQueriesPlot,
  selectorLastError,
  selectorLiveMode,
  selectorLoadMetricsList,
  selectorMetricsList,
  selectorMetricsMetaByName,
  selectorParams,
  selectorPreviews,
  selectorTimeRange,
  selectorTitle,
  useStore,
} from '../store';
import { now } from './utils';
import { debug } from '../common/debug';
import { PlotParams } from '../common/plotQueryParams';

const {
  setPlotParams,
  setTimeRange,
  updateParamsByUrl,
  initSetSearchParams,
  setBaseRange,
  setCompact,
  setLastError,
  loadMetricsMeta,
} = useStore.getState();

const clearLastError = () => {
  setLastError('');
};

export type ViewPageProps = {
  embed?: boolean;
  yAxisSize?: number;
};
export const ViewPage: React.FC<ViewPageProps> = ({ embed, yAxisSize = 54 }) => {
  const [rawParams, setRawParams] = useSearchParams();
  const title = useStore(selectorTitle);
  const params = useStore(selectorParams);
  const activePlot: PlotParams | undefined = params.plots[params.tabNum];
  const timeRange = useStore(selectorTimeRange);

  const live = useStore(selectorLiveMode);

  const plotPreviews = useStore(selectorPreviews);

  const numQueries = useStore(selectorGlobalNumQueriesPlot);

  const metricsOptions = useStore(selectorMetricsList);
  const loadMetricsList = useStore(selectorLoadMetricsList);

  const lastError = useStore(selectorLastError);
  const plotPreview = plotPreviews[params.tabNum];

  const selectorActivePlotMetricsMeta = useMemo(
    () => selectorMetricsMetaByName.bind(undefined, activePlot?.metricName ?? ''),
    [activePlot]
  );
  const meta = useStore(selectorActivePlotMetricsMeta);

  useEffect(() => {
    initSetSearchParams(setRawParams);
  }, [setRawParams]);

  useEffect(() => {
    updateParamsByUrl();
  }, [rawParams]);

  useEffect(() => {
    loadMetricsList();
  }, [loadMetricsList]);

  useEffect(() => {
    if (activePlot?.metricName) {
      loadMetricsMeta(activePlot.metricName);
    }
  }, [activePlot]);

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
      if (link.href.slice(0, 5) === 'blob:' && link.href !== data) {
        URL.revokeObjectURL(link.href);
      }
      link.href = data || '/favicon.ico';
    });
  }, [params.tabNum, plotPreview]);

  useEffect(() => {
    if (title) {
      document.title = title;
    }
  }, [title]);

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
    if (live) {
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
  }, [live, refresh, timeRange.relativeFrom]);

  if (params.plots.length === 0) {
    return (
      <div
        hidden={!lastError}
        className="alert alert-danger d-flex align-items-center justify-content-between"
        role="alert"
      >
        <small className="overflow-force-wrap font-monospace">{lastError}</small>
        <button type="button" className="btn-close" aria-label="Close" onClick={clearLastError} />
      </div>
    );
  }
  return (
    <div className="d-flex flex-column flex-md-row">
      <div className={embed ? 'flex-grow-1' : 'flex-grow-1 pt-3 pb-3'}>
        <div className="tab-content position-relative">
          {params.tabNum >= 0 && (
            <div className={`container-xl tab-pane show active ${params.tabNum >= 0 ? '' : 'hidden-dashboard'}`}>
              <PlotLayout
                embed={embed}
                indexPlot={params.tabNum}
                setParams={setPlotParams}
                metricsOptions={metricsOptions}
                sel={activePlot}
                meta={meta}
                numQueries={numQueries}
                setBaseRange={setBaseRange}
              >
                {params.plots.map((plot, index) => (
                  <PlotView
                    key={index}
                    indexPlot={index}
                    type={plot.type}
                    compact={!!embed}
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
