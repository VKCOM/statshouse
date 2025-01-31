// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { DataQuery, DataSourceJsonData } from '@grafana/data';

export type KeysMap = {
  [id: string]: Key;
};

export interface SHQuery extends DataQuery {
  metricName: string;
  func: string;
  what: string[];
  keys: KeysMap;
  shifts: number[];
  topN: number;
  mode: string;
  url: string;
  alias: string;
}

export const defaultQuery: Partial<SHQuery> = {
  metricName: 'api_methods',
  shifts: [],
  topN: 5,
  mode: 'builder',
  what: [],
  alias: '',
};

export interface VariableQuery {
  url: string;
}

/**
 * These are options configured for each DataSource instance.
 */
export interface SHDataSourceOptions extends DataSourceJsonData {
  apiURL?: string;
}

/**
 * Value that is used in the backend, but never sent over HTTP to the frontend
 */
export interface SHSecureJsonData {
  apiKey?: string;
}

export type MetricNamesResponse = {
  metric_names: string[];
};

export type MetricPropertiesResponse = {
  functions: string[];
  tags: Tag[];
};

export type MetricTagValuesResponse = {
  tag_values: TagValue[];
};

export type Key = {
  values: string[];
  groupBy: boolean;
  notIn: boolean;
  raw: boolean;
};

export type Tag = {
  id: string;
  description?: string;
  isRaw: boolean;
};

export type TagValue = {
  value: string;
  count: number;
};
