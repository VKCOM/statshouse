// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { SetStateAction, useCallback, useEffect, useMemo } from 'react';
import { useSearchParams } from 'react-router-dom';
import produce from 'immer';
import { querySelector } from './api';
import { Dashboard, PlotLayout } from '../components';
import { PlotParams } from '../common/plotQueryParams';
import { setBackgroundColor } from '../common/canvasToImage';
import {
  selectorGlobalNumQueriesPlot,
  selectorInitSetSearchParams,
  selectorLastError,
  selectorLiveMode,
  selectorLoadMetricsList,
  selectorLoadMetricsMeta,
  selectorMetricsList,
  selectorMetricsMetaByName,
  selectorParams,
  selectorPlotActive,
  selectorPreviews,
  selectorSetBaseRange,
  selectorSetCompact,
  selectorSetLastError,
  selectorSetParams,
  selectorSetTimeRange,
  selectorTimeRange,
  selectorTitle,
  selectorUpdateParamsByUrl,
  useStore,
} from '../store';
import PlotView from './PlotView';
import { now } from './utils';
import { debug } from '../common/debug';

export type ViewPageProps = {
  embed?: boolean;
  yAxisSize?: number;
};
export const ViewPage: React.FC<ViewPageProps> = ({ embed, yAxisSize = 54 }) => {
  const [rawParams, setRawParams] = useSearchParams();
  const title = useStore(selectorTitle);
  const params = useStore(selectorParams);
  const activePlot = useStore(selectorPlotActive);
  const setParams = useStore(selectorSetParams);
  const timeRange = useStore(selectorTimeRange);
  const setTimeRange = useStore(selectorSetTimeRange);
  const updateParamsByUrl = useStore(selectorUpdateParamsByUrl);
  const initSetSearchParams = useStore(selectorInitSetSearchParams);
  const live = useStore(selectorLiveMode);

  const plotPreviews = useStore(selectorPreviews);

  const setBaseRange = useStore(selectorSetBaseRange);

  const numQueries = useStore(selectorGlobalNumQueriesPlot);

  const setCompact = useStore(selectorSetCompact);

  const metricsOptions = useStore(selectorMetricsList);
  const loadMetricsList = useStore(selectorLoadMetricsList);

  const lastError = useStore(selectorLastError);
  const setLastError = useStore(selectorSetLastError);

  const clearLastError = useCallback(() => {
    setLastError('');
  }, [setLastError]);

  const selectorActivePlotMetricsMeta = useMemo(
    () => selectorMetricsMetaByName.bind(undefined, activePlot?.metricName ?? ''),
    [activePlot]
  );
  const meta = useStore(selectorActivePlotMetricsMeta);
  const loadMetricsMeta = useStore(selectorLoadMetricsMeta);

  useEffect(() => {
    initSetSearchParams(setRawParams);
  }, [initSetSearchParams, setRawParams]);

  useEffect(() => {
    updateParamsByUrl();
  }, [rawParams, updateParamsByUrl]);

  useEffect(() => {
    loadMetricsList();
  }, [loadMetricsList]);

  useEffect(() => {
    if (activePlot?.metricName) {
      loadMetricsMeta(activePlot.metricName);
    }
  }, [activePlot, loadMetricsMeta]);

  useEffect(() => {
    setCompact(!!embed);
  }, [embed, setCompact]);

  useEffect(() => {
    setBackgroundColor(plotPreviews[params.tabNum] ?? '', 'rgba(255,255,255,1)', 64).then((data) => {
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
  }, [params.tabNum, plotPreviews]);

  const changeParams = useCallback(
    (index: number, sel: SetStateAction<querySelector>, forceReplace?: boolean) => {
      if (typeof index !== 'undefined') {
        setParams(
          produce((p) => {
            p.plots[index] = (typeof sel === 'function' ? sel(p.plots[index]) : sel) as PlotParams;
          }),
          forceReplace
        );
      }
    },
    [setParams]
  );

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
  }, [setTimeRange]);

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
                setParams={changeParams}
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
