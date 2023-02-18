// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import {
  queryDashboardGroupInfoCount,
  queryDashboardGroupInfoName,
  queryDashboardGroupInfoPrefix,
  queryDashboardGroupInfoShow,
  queryDashboardID,
  queryParamAgg,
  queryParamBackendVersion,
  queryParamFilter,
  queryParamFilterSync,
  queryParamFromTime,
  queryParamGroupBy,
  queryParamLive,
  queryParamLockMax,
  queryParamLockMin,
  queryParamMaxHost,
  queryParamMetric,
  queryParamNumResults,
  queryParamTabNum,
  queryParamTimeShifts,
  queryParamToTime,
  queryParamWhat,
  queryWhat,
  tabPrefix,
} from '../view/api';
import { defaultTimeRange, KeysTo, TIME_RANGE_KEYS_TO } from './TimeRange';
import {
  BooleanParam,
  ConfigParams,
  decodeQueryParams,
  encodeQueryParams,
  FilterParams,
  NumberParam,
  TagSyncParam,
  TimeToParam,
  UseV2Param,
} from './QueryParamsParser';
import { getNextState } from '../store';
import React from 'react';

export interface lockRange {
  readonly min: number;
  readonly max: number;
}

export type PlotParams = {
  metricName: string;
  what: queryWhat[];
  customAgg: number;
  groupBy: string[];
  filterIn: Record<string, string[]>;
  filterNotIn: Record<string, string[]>;
  numSeries: number;
  useV2: boolean;
  yLock: {
    min: number;
    max: number;
  };
  maxHost: boolean;
};

export type GroupInfo = {
  name: string;
  show: boolean;
  count: number;
};

export type DashboardParams = {
  dashboard_id?: number;
  name?: string;
  description?: string;
  version?: number;
  /**
   * @deprecated not use
   */
  groups?: (number | null)[]; // [page_plot]
  groupInfo?: GroupInfo[];
};

export type QueryParams = {
  ['@type']?: 'QueryParams'; // ld-json
  dashboard?: DashboardParams;
  timeRange: { to: number | KeysTo; from: number };
  timeShifts: number[];
  tabNum: number;
  tagSync: (number | null)[][]; // [group][page_plot][tag_index]
  plots: PlotParams[];
};

export const defaultParams: Readonly<QueryParams> = {
  dashboard: {
    dashboard_id: undefined,
  },
  timeRange: { ...defaultTimeRange },
  tabNum: 0,
  timeShifts: [],
  tagSync: [],
  plots: [
    // {
    //   metricName: '',
    //   what: ['count_norm'],
    //   customAgg: 0,
    //   groupBy: [],
    //   filterIn: {},
    //   filterNotIn: {},
    //   numSeries: 5,
    //   timeShifts: [],
    //   useV2: true,
    //   yLock: {
    //     min: 0,
    //     max: 0,
    //   },
    // },
  ],
};

export function parseParamsFromUrl(url: string): QueryParams {
  return decodeQueryParams(configParams, defaultParams, new URLSearchParams(new URL(url).search))!;
}

export function getUrlSearch(nextState: React.SetStateAction<QueryParams>, prev?: QueryParams, baseLink?: string) {
  const params = new URLSearchParams(baseLink ?? window.location.search);
  const prevState = prev ?? decodeQueryParams(configParams, defaultParams, params);
  if (prevState) {
    const newState = getNextState(prevState, nextState);
    return '?' + encodeQueryParams(configParams, newState, defaultParams, params).toString();
  }
  return '?' + params.toString();
}

export function sortEntity<T extends number | string>(arr: T[]): T[] {
  return [...arr].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
}

export function readDashboardID(params: URLSearchParams): number | undefined {
  return decodeQueryParams<{ dashboard_id: number }>(configDashboardId, undefined, params)?.dashboard_id;
}

export function getLiveParams(params: URLSearchParams, defaultParams: boolean = false): boolean {
  return (
    decodeQueryParams<{ liveMode: boolean }>(configLive, { liveMode: defaultParams }, params)?.liveMode ?? defaultParams
  );
}

export function setLiveParams(
  value: boolean,
  params: URLSearchParams,
  defaultParams: boolean = false
): URLSearchParams {
  return encodeQueryParams<{ liveMode: boolean }>(configLive, { liveMode: value }, { liveMode: defaultParams }, params);
}
/**
 * url get param parser config only dashboard_id
 * /view?id=144155682
 */
export const configDashboardId: ConfigParams = {
  dashboard_id: {
    ...NumberParam,
    always: true,
    default: undefined,
    urlKey: queryDashboardID,
  },
};

/**
 * url get param parser config only liveMode
 * /view?live=1
 */
export const configLive: ConfigParams = {
  liveMode: {
    ...BooleanParam,
    default: false,
    urlKey: queryParamLive,
  },
};
/**
 * url get param parser config
 * /view?...
 */
export const configParams: ConfigParams = {
  tabNum: {
    /**
     * tab page
     * tn=-2 - dashboard setting
     * tn=-1 - dashboard
     * tn=0 - plot number 1
     */
    ...NumberParam,
    default: 0,
    urlKey: queryParamTabNum,
  },
  dashboard: {
    /**
     * dashboard config
     * id=144155682&
     */
    params: {
      /**
       * dashboard id
       * id=144155682
       */
      dashboard_id: {
        ...NumberParam,
        always: true,
        default: undefined,
        urlKey: queryDashboardID,
      },
      /**
       * dashboard name
       */
      name: {
        default: '',
        urlKey: '', //not exist in url
      },
      /**
       * dashboard description
       */
      description: {
        default: '',
        urlKey: '', //not exist in url
      },
      /**
       * dashboard version
       */
      version: {
        ...NumberParam,
        default: 0,
        urlKey: '', //not exist in url
      },
      /**
       * dashboard group info
       * g0.t=group+0&g.gs=1&gi1.gn=group+1&gi1.gs=0
       */
      groupInfo: {
        default: [],
        isArray: true,
        struct: true,
        prefixArray: (i) => `${queryDashboardGroupInfoPrefix}${i}.`, // first group not num prefix
        params: {
          name: {
            required: true,
            urlKey: queryDashboardGroupInfoName,
          },
          show: {
            ...BooleanParam,
            default: true,
            urlKey: queryDashboardGroupInfoShow,
          },
          count: {
            ...NumberParam,
            default: 0,
            urlKey: queryDashboardGroupInfoCount,
          },
        },
      },
    },
  },
  timeRange: {
    /**
     * t=1675065875&f=-900
     */
    default: {
      to: TIME_RANGE_KEYS_TO.default,
      from: 0,
    },
    params: {
      to: {
        /**
         * t>0 - absolute time
         * t<0 - relative time now - abs(t second)
         * t=0 - relative now
         * t=ed - relative end day
         * t=ew - relative week
         * t=d - absolute now time
         */
        ...TimeToParam,
        default: TIME_RANGE_KEYS_TO.default,
        urlKey: queryParamToTime,
      },
      from: {
        /**
         * f>0 - absolute time
         * f<0 - relative time [time to] - abs(f second)
         */
        ...NumberParam,
        default: 0,
        urlKey: queryParamFromTime,
      },
    },
  },
  plots: {
    /**
     * plots filter info
     * s=metric&qw=avg&qf=0-tag_value&t1.s=metric2&t1.qf=0-tag_value2
     */
    default: [],
    isArray: true,
    struct: true,
    prefixArray: (i) => (i ? `${tabPrefix}${i}.` : ''), //first plot not prefix
    params: {
      metricName: {
        urlKey: queryParamMetric, // s=metric or t1.s=metric2
        required: true,
      },
      what: {
        urlKey: queryParamWhat, // qw=avg or qw=count
        default: ['count_norm'] as queryWhat[],
        isArray: true,
      },
      customAgg: {
        ...NumberParam,
        urlKey: queryParamAgg, // g=1 or t1.g=5
        default: 0,
      },
      groupBy: {
        urlKey: queryParamGroupBy, //qb=key0 or t1.qb=key1
        default: [] as string[],
        isArray: true,
      },
      filterIn: {
        ...FilterParams(),
        urlKey: queryParamFilter, //qf=0-tag_value or t1.qf=1-tag_value2
        isArray: true,
        fromEntries: true,
        default: {} as Record<string, string[]>,
      },
      filterNotIn: {
        ...FilterParams(true),
        urlKey: queryParamFilter, //qf=0~tag_value or t1.qf=1~tag_value2
        isArray: true,
        fromEntries: true,
        default: {} as Record<string, string[]>,
      },
      numSeries: {
        /**
         * top N series
         */
        ...NumberParam,
        urlKey: queryParamNumResults, //n=5 or t1.n=10
        default: 5,
      },
      useV2: {
        /**
         * api version
         */
        ...UseV2Param,
        urlKey: queryParamBackendVersion, // v=2 or t1.v=1
        default: true,
      },
      yLock: {
        /**
         * y lock plot
         */
        params: {
          min: {
            ...NumberParam,
            urlKey: queryParamLockMin, // yl=1010322.5806451612 or t1.yl=1010322.5806451612
            default: 0,
          },
          max: {
            ...NumberParam,
            urlKey: queryParamLockMax, // yh=1637419.3548387096 or t1.yh=1637419.3548387096
            default: 0,
          },
        },
      },
      maxHost: {
        ...BooleanParam,
        urlKey: queryParamMaxHost,
        default: false,
      },
    },
  },
  timeShifts: {
    /**
     * add time shift all series
     */
    ...NumberParam,
    urlKey: queryParamTimeShifts, //ts=-86400 or t1.ts=-172800
    default: [] as number[],
    isArray: true,
  },
  tagSync: {
    /**
     * sync tag from dashboard
     * fs=0.0-1.0
     * [plot id].[tag index]-[plot id].[tag index]
     */
    ...TagSyncParam,
    urlKey: queryParamFilterSync, // fs=0.0-1.0
    default: [],
  },
};
