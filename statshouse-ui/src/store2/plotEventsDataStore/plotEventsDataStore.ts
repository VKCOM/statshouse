// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { StoreSlice } from '../createStore';
import { autoAgg, autoLowAgg, StatsHouseStore } from '@/store2';
import { GET_PARAMS, METRIC_TYPE, PLOT_TYPE, QueryWhat } from '@/api/enum';
import {
  freeKeyPrefix,
  getTimeRangeAbsolute,
  metricFilterEncode,
  PlotKey,
  promQLMetric,
  type QueryParams,
  readTimeRange,
  TimeRange,
} from '@/url2';
import { querySeriesMetaTag } from '@/view/api';
import { replaceVariable } from '../helpers/replaceVariable';
import { apiTable, ApiTableGet, GetTableResp, QueryTableRow } from '@/api/table';
import { uniqueArray } from '@/common/helpers';
import { formatByMetricType, getMetricType } from '@/common/formatByMetricType';
import { debug } from '@/common/debug';
import { fmtInputDateTime, formatLegendValue } from '@/view/utils2';
import { useLiveModeStore } from '../liveModeStore';
import { ExtendedError } from '../../api/api';

type EventDataChunk = GetTableResp & { to: number; from: number; fromEnd: boolean };

export type EventDataRow = {
  key: string;
  idChunk: number;
  timeString: string;
  time: number;
  data: number[];
} & Partial<Record<string, querySeriesMetaTag>>;

export type EventData = {
  chunks: EventDataChunk[];
  rows: EventDataRow[];
  what: QueryWhat[];
  nextKey?: string;
  prevKey?: string;
  range: TimeRange;
  nextAbortController?: AbortController;
  prevAbortController?: AbortController;
  error?: string;
  error403?: string;
};

export type PlotEventsDataStore = {
  plotsEventsData: Partial<Record<PlotKey, EventData>>;
  loadPlotEvents(plotKey: PlotKey, key?: string, fromEnd?: boolean, from?: number): Promise<EventData | null>;
  clearPlotEvents(plotKey: PlotKey): void;
};

export const plotEventsDataStore: StoreSlice<StatsHouseStore, PlotEventsDataStore> = (setState, getState) => ({
  plotsEventsData: {},
  async loadPlotEvents(plotKey, key, fromEnd = false, from) {
    if (!getState().plotsEventsData[plotKey]) {
      setState((state) => {
        state.plotsEventsData[plotKey] = getEmptyPlotEventsData(state.params.timeRange);
      });
    }
    const prevState = getState();
    const prevEvent = prevState.plotsEventsData[plotKey];
    const prevPlot = prevState.params.plots[plotKey];
    const compact = prevState.isEmbed;
    if (compact || prevPlot?.type !== PLOT_TYPE.Event || prevPlot?.metricName === promQLMetric) {
      return null;
    }
    if (fromEnd) {
      prevEvent?.prevAbortController?.abort();
    } else {
      prevEvent?.nextAbortController?.abort();
    }
    const controller = new AbortController();

    const { status, interval } = useLiveModeStore.getState();
    const intervalParam = status ? interval : undefined;
    const params = prevState.params;
    const plot = params.plots[plotKey];

    if (plot) {
      setState((state) => {
        const plotEventsData = state.plotsEventsData[plotKey];
        if (plotEventsData) {
          if (fromEnd) {
            plotEventsData.prevAbortController = controller;
          } else {
            plotEventsData.nextAbortController = controller;
          }
        }
      });

      const { response, error, status } = await apiTable(plot, params, intervalParam, key, fromEnd);

      if (response) {
        await getState().loadMetricMeta(getState().params.plots[plotKey]?.metricName ?? '');
        setState((state) => {
          const plotEventsData = (state.plotsEventsData[plotKey] ??= getEmptyPlotEventsData(
            prevState.params.timeRange
          ));

          const meta = state.metricMeta[prevPlot.metricName];
          const metricType = getMetricType(prevPlot.what, prevPlot.metricUnit ?? meta?.metric_type);
          const formatMetric = metricType !== METRIC_TYPE.none && formatByMetricType(metricType);
          const range = getTimeRangeAbsolute(prevState.params.timeRange);
          const chunk: EventDataChunk = {
            ...response.data,
            ...range,
            fromEnd,
            rows:
              response.data.rows?.map(
                (value) =>
                  ({
                    ...value,
                    tags:
                      value.tags &&
                      Object.fromEntries(
                        Object.entries(value.tags).map(([tagKey, tagValue]) => [freeKeyPrefix(tagKey), tagValue])
                      ),
                  }) as QueryTableRow
              ) ?? null,
          };
          if (chunk.more) {
            if (chunk.fromEnd) {
              chunk.from = chunk.rows?.[0]?.time ?? range.from;
            } else {
              chunk.to = chunk.rows?.[chunk.rows?.length - 1]?.time ?? range.to;
            }
          }
          if (key) {
            if (fromEnd) {
              plotEventsData.chunks.unshift(chunk);
            } else {
              plotEventsData.chunks.push(chunk);
            }
          } else {
            plotEventsData.chunks = [chunk];
          }
          plotEventsData.what = chunk.what;
          plotEventsData.rows = plotEventsData.chunks.flatMap(
            (chunk, idChunk) =>
              chunk.rows?.map(
                (row, index): EventDataRow =>
                  ({
                    key: `${chunk.from_row}_${index}`,
                    idChunk,
                    timeString: fmtInputDateTime(new Date(row.time * 1000)),
                    data: row.data,
                    time: row.time,
                    ...Object.fromEntries(
                      plotEventsData.what.map((whatKey, indexWhat) => [
                        whatKey,
                        {
                          value: row.data[indexWhat],
                          formatValue: formatMetric
                            ? formatMetric(row.data[indexWhat])
                            : formatLegendValue(row.data[indexWhat]),
                        },
                      ])
                    ),
                    ...row.tags,
                  }) as EventDataRow
              ) ?? []
          );

          const first = plotEventsData.chunks[0];
          if ((first?.more && first?.fromEnd) || from) {
            plotEventsData.prevKey = first?.from_row;
          } else {
            plotEventsData.prevKey = undefined;
          }
          const last = plotEventsData.chunks[plotEventsData.chunks.length - 1];
          if (last?.more && !last?.fromEnd) {
            plotEventsData.nextKey = last?.to_row;
          } else {
            plotEventsData.nextKey = undefined;
          }

          plotEventsData.range = readTimeRange(
            plotEventsData.chunks[0]?.from ?? range.from,
            plotEventsData.chunks[plotEventsData.chunks.length - 1]?.to ?? range.to
          );
          plotEventsData.error = undefined;
          plotEventsData.error403 = undefined;
        });
      }
      if (error) {
        setState((state) => {
          const plotEventsData = (state.plotsEventsData[plotKey] = getEmptyPlotEventsData(state.params.timeRange));
          if (status === 403) {
            plotEventsData.error403 = error.toString();
          } else if (error.name !== 'AbortError' && error.status !== ExtendedError.ERROR_STATUS_UNKNOWN) {
            debug.error(error);
            plotEventsData.error = error.toString();
          }
        });
      }
      setState((state) => {
        const plotEventsData = state.plotsEventsData[plotKey];
        if (plotEventsData) {
          if (fromEnd) {
            plotEventsData.prevAbortController = undefined;
          } else {
            plotEventsData.nextAbortController = undefined;
          }
        }
      });
      return getState().plotsEventsData[plotKey] ?? null;
    }
    return null;
  },

  clearPlotEvents(plotKey) {
    setState((state) => {
      state.plotsEventsData[plotKey] = { chunks: [], rows: [], what: [], range: { ...state.params.timeRange } };
    });
  },
});

export function getLoadTableUrlParams(
  plotKey: PlotKey,
  params: QueryParams,
  interval?: number,
  key?: string,
  fromEnd: boolean = false,
  limit: number = 1000
): ApiTableGet | null {
  let plot = params.plots[plotKey];
  if (!plot || plot.metricName === promQLMetric) {
    return null;
  }
  plot = replaceVariable(plotKey, plot, params.variables);
  const width = plot.customAgg === -1 ? autoLowAgg : plot.customAgg === 0 ? autoAgg : `${plot.customAgg}s`;
  const urlParams: ApiTableGet = {
    [GET_PARAMS.metricName]: plot.metricName,
    [GET_PARAMS.numResults]: plot.numSeries.toString(),
    [GET_PARAMS.metricWhat]: plot.what.slice(),
    [GET_PARAMS.toTime]: params.timeRange.to.toString(),
    [GET_PARAMS.fromTime]: params.timeRange.from.toString(),
    [GET_PARAMS.width]: width.toString(),
    [GET_PARAMS.version]: plot.backendVersion,
    [GET_PARAMS.metricFilter]: metricFilterEncode('', plot.filterIn, plot.filterNotIn).map(([, v]) => v),
    [GET_PARAMS.metricGroupBy]: uniqueArray([...plot.groupBy.map(freeKeyPrefix), ...plot.eventsBy]),
    // [GET_PARAMS.metricAgg]: plot.customAgg.toString(),
    // [GET_PARAMS.metricTimeShifts]: params.timeShifts.map((t) => t.toString()),
    // [GET_PARAMS.excessPoints]: '1',
    // [GET_PARAMS.metricVerbose]: fetchBadges ? '1' : '0',
  };

  if (fromEnd) {
    urlParams[GET_PARAMS.metricFromEnd] = '1';
  }
  if (key) {
    if (fromEnd) {
      urlParams[GET_PARAMS.metricToRow] = key;
    } else {
      urlParams[GET_PARAMS.metricFromRow] = key;
    }
  }
  urlParams[GET_PARAMS.numResults] = limit.toString();
  // todo:
  // if (allParams) {
  //   urlParams.push(...encodeVariableValues(allParams));
  //   urlParams.push(...encodeVariableConfig(allParams));
  // }

  if (interval) {
    urlParams[GET_PARAMS.metricLive] = interval?.toString();
  }

  return urlParams;
}

export function getEmptyPlotEventsData(timeRange: TimeRange): EventData {
  return {
    chunks: [],
    rows: [],
    what: [],
    range: { ...timeRange },
  };
}
