// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { defaultTimeRange, KeysTo, TIME_RANGE_KEYS_TO } from './TimeRange';
import {
  BooleanParam,
  ConfigParams,
  decodeQueryParams,
  encodeQueryParams,
  FilterParams,
  GroupByParams,
  NumberParam,
  TagSyncParam,
  TimeToParam,
  UseV2Param,
} from './QueryParamsParser';
import { getNextState } from '../store';
import React from 'react';
import { GET_PARAMS, QueryWhat } from '../api/enum';

export interface lockRange {
  readonly min: number;
  readonly max: number;
}

export const PLOT_TYPE = {
  Metric: 0,
  Event: 1,
} as const;
export type PlotType = (typeof PLOT_TYPE)[keyof typeof PLOT_TYPE];

export type PlotParams = {
  metricName: string;
  customName: string;
  what: QueryWhat[];
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
  promQL: string;
  type: PlotType;
  events: number[];
  eventsBy: string[];
  eventsHide: string[];
};

export type GroupInfo = {
  name: string;
  show: boolean;
  count: number;
  size: number;
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
  eventFrom: number;
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
  eventFrom: 0,
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

export function getUrlSearch(
  nextState: React.SetStateAction<QueryParams>,
  prev?: QueryParams,
  baseLink?: string,
  defaultP?: QueryParams
) {
  const params = new URLSearchParams(baseLink ?? window.location.search);
  const prevState = prev ?? decodeQueryParams(configParams, defaultP ?? defaultParams, params);
  if (prevState) {
    const newState = getNextState(prevState, nextState);
    return '?' + encodeQueryParams(configParams, newState, defaultP ?? defaultParams, params).toString();
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
    urlKey: GET_PARAMS.dashboardID,
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
    urlKey: GET_PARAMS.metricLive,
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
    urlKey: GET_PARAMS.metricTabNum,
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
        urlKey: GET_PARAMS.dashboardID,
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
        prefixArray: (i) => `${GET_PARAMS.dashboardGroupInfoPrefix}${i}.`, // first group not num prefix
        params: {
          name: {
            required: true,
            urlKey: GET_PARAMS.dashboardGroupInfoName,
          },
          show: {
            ...BooleanParam,
            default: true,
            urlKey: GET_PARAMS.dashboardGroupInfoShow,
          },
          count: {
            ...NumberParam,
            default: 0,
            urlKey: GET_PARAMS.dashboardGroupInfoCount,
          },
          size: {
            ...NumberParam,
            default: 2,
            urlKey: GET_PARAMS.dashboardGroupInfoSize,
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
        urlKey: GET_PARAMS.toTime,
      },
      from: {
        /**
         * f>0 - absolute time
         * f<0 - relative time [time to] - abs(f second)
         */
        ...NumberParam,
        default: 0,
        urlKey: GET_PARAMS.fromTime,
      },
    },
  },
  eventFrom: {
    ...NumberParam,
    default: 0,
    urlKey: GET_PARAMS.metricEventFrom,
  },
  plots: {
    /**
     * plots filter info
     * s=metric&qw=avg&qf=0-tag_value&t1.s=metric2&t1.qf=0-tag_value2
     */
    default: [],
    isArray: true,
    struct: true,
    prefixArray: (i) => (i ? `${GET_PARAMS.plotPrefix}${i}.` : ''), //first plot not prefix
    params: {
      metricName: {
        urlKey: GET_PARAMS.metricName, // s=metric or t1.s=metric2
        required: true,
      },
      customName: {
        urlKey: GET_PARAMS.metricCustomName,
        default: '',
      },
      what: {
        urlKey: GET_PARAMS.metricWhat, // qw=avg or qw=count
        default: ['count_norm'] as QueryWhat[],
        isArray: true,
      },
      customAgg: {
        ...NumberParam,
        urlKey: GET_PARAMS.metricAgg, // g=1 or t1.g=5
        default: 0,
      },
      groupBy: {
        ...GroupByParams,
        urlKey: GET_PARAMS.metricGroupBy, //qb=key0 or t1.qb=key1
        default: [] as string[],
        isArray: true,
      },
      filterIn: {
        ...FilterParams(),
        urlKey: GET_PARAMS.metricFilter, //qf=0-tag_value or t1.qf=1-tag_value2
        isArray: true,
        fromEntries: true,
        default: {} as Record<string, string[]>,
      },
      filterNotIn: {
        ...FilterParams(true),
        urlKey: GET_PARAMS.metricFilter, //qf=0~tag_value or t1.qf=1~tag_value2
        isArray: true,
        fromEntries: true,
        default: {} as Record<string, string[]>,
      },
      numSeries: {
        /**
         * top N series
         */
        ...NumberParam,
        urlKey: GET_PARAMS.numResults, //n=5 or t1.n=10
        default: 5,
      },
      useV2: {
        /**
         * api version
         */
        ...UseV2Param,
        urlKey: GET_PARAMS.version, // v=2 or t1.v=1
        default: true,
      },
      yLock: {
        /**
         * y lock plot
         */
        params: {
          min: {
            ...NumberParam,
            urlKey: GET_PARAMS.metricLockMin, // yl=1010322.5806451612 or t1.yl=1010322.5806451612
            default: 0,
          },
          max: {
            ...NumberParam,
            urlKey: GET_PARAMS.metricLockMax, // yh=1637419.3548387096 or t1.yh=1637419.3548387096
            default: 0,
          },
        },
      },
      maxHost: {
        ...BooleanParam,
        urlKey: GET_PARAMS.metricMaxHost,
        default: false,
      },
      promQL: { default: '', urlKey: GET_PARAMS.metricPromQL },
      type: {
        ...NumberParam,
        default: PLOT_TYPE.Metric,
        urlKey: GET_PARAMS.metricType,
      },
      events: {
        ...NumberParam,
        isArray: true,
        default: [],
        urlKey: GET_PARAMS.metricEvent,
      },
      eventsBy: {
        urlKey: GET_PARAMS.metricEventBy, //eb=0 or t1.eb=1
        default: [] as string[],
        isArray: true,
      },
      eventsHide: {
        urlKey: GET_PARAMS.metricEventHide, //eh=0 or t1.eh=1
        default: [] as string[],
        isArray: true,
      },
    },
  },
  timeShifts: {
    /**
     * add time shift all series
     */
    ...NumberParam,
    urlKey: GET_PARAMS.metricTimeShifts, //ts=-86400 or t1.ts=-172800
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
    urlKey: GET_PARAMS.metricFilterSync, // fs=0.0-1.0
    default: [],
  },
};
