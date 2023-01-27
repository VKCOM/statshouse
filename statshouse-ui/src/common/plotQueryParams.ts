// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import {
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
  queryParamMetric,
  queryParamNumResults,
  queryParamTabNum,
  queryParamTimeShifts,
  queryParamToTime,
  queryParamWhat,
  queryWhat,
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
  timeShifts: number[];
  useV2: boolean;
  yLock: {
    min: number;
    max: number;
  };
};

export type GroupInfo = {
  name: string;
  show: boolean;
};

export type DashboardParams = {
  dashboard_id?: number;
  name?: string;
  description?: string;
  version?: number;
  groups?: (number | null)[]; // [page_plot]
  groupInfo?: GroupInfo[];
};

export type QueryParams = {
  ['@type']?: 'QueryParams'; // ld-json
  dashboard?: DashboardParams;
  timeRange: { to: number | KeysTo; from: number };
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

export const configDashboardId: ConfigParams = {
  dashboard_id: {
    ...NumberParam,
    always: true,
    default: undefined,
    urlKey: queryDashboardID,
  },
};
export const configLive: ConfigParams = {
  liveMode: {
    ...BooleanParam,
    default: false,
    urlKey: queryParamLive,
  },
};

export const configParams: ConfigParams = {
  tabNum: {
    ...NumberParam,
    default: 0,
    urlKey: queryParamTabNum,
  },
  dashboard: {
    params: {
      dashboard_id: {
        ...NumberParam,
        always: true,
        default: undefined,
        urlKey: queryDashboardID,
      },
      name: {
        default: '',
        urlKey: '',
      },
      description: {
        default: '',
        urlKey: '',
      },
      version: {
        ...NumberParam,
        default: 0,
        urlKey: '',
      },
      groups: {
        default: [],
        urlKey: '',
      },
      groupInfo: {
        default: [],
        isArray: true,
        // prefixArray: (i) => `gi${i ? i : ''}.`,
        prefixArray: () => '',
        params: {
          name: {
            required: true,
            default: '',
            urlKey: '',
          },
          show: {
            ...BooleanParam,
            default: true,
            urlKey: '',
          },
        },
      },
    },
  },
  plots: {
    default: [],
    isArray: true,
    prefixArray: (i) => (i ? `t${i}.` : ''),
    params: {
      metricName: {
        urlKey: queryParamMetric,
        required: true,
      },
      what: {
        urlKey: queryParamWhat,
        default: ['count_norm'] as queryWhat[],
        isArray: true,
      },
      customAgg: {
        urlKey: queryParamAgg,
        default: 0,
      },
      groupBy: {
        urlKey: queryParamGroupBy,
        default: [] as string[],
        isArray: true,
      },
      filterIn: {
        ...FilterParams(),
        urlKey: queryParamFilter,
        isArray: true,
        fromEntries: true,
        default: {} as Record<string, string[]>,
      },
      filterNotIn: {
        ...FilterParams(true),
        urlKey: queryParamFilter,
        isArray: true,
        fromEntries: true,
        default: {} as Record<string, string[]>,
      },
      numSeries: {
        ...NumberParam,
        urlKey: queryParamNumResults,
        default: 5,
      },
      timeShifts: {
        ...NumberParam,
        urlKey: queryParamTimeShifts,
        default: [] as number[],
        isArray: true,
      },
      useV2: {
        ...UseV2Param,
        urlKey: queryParamBackendVersion,
        default: true,
      },
      yLock: {
        params: {
          min: {
            ...NumberParam,
            urlKey: queryParamLockMin,
            default: 0,
          },
          max: {
            ...NumberParam,
            urlKey: queryParamLockMax,
            default: 0,
          },
        },
      },
    },
  },
  timeRange: {
    default: {
      to: TIME_RANGE_KEYS_TO.default,
      from: 0,
    },
    params: {
      to: {
        ...TimeToParam,
        default: TIME_RANGE_KEYS_TO.default,
        urlKey: queryParamToTime,
      },
      from: {
        ...NumberParam,
        default: 0,
        urlKey: queryParamFromTime,
      },
    },
  },
  tagSync: {
    ...TagSyncParam,
    urlKey: queryParamFilterSync,
    default: [],
  },
};
