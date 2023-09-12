// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { QueryWhat, TagKey } from '../api/enum';
import { normalizeFilterKey } from '../url/queryParams';

export interface settings {
  readonly vkuth_app_name?: string;
  readonly default_metric: string;
  readonly default_metric_group_by: readonly TagKey[];
  readonly default_metric_filter_in: Readonly<Partial<Record<TagKey, string[]>>>;
  readonly default_metric_filter_not_in: Readonly<Partial<Record<TagKey, string[]>>>;
  readonly default_metric_what: readonly QueryWhat[];
  readonly default_num_series: number;
  readonly disabled_v1: boolean;
  readonly skip_error_code: number[];
  readonly skip_error_count: number;
}

const defaultSettings: settings = {
  vkuth_app_name: '',
  default_metric: '__agg_bucket_receive_delay_sec',
  default_metric_group_by: [],
  default_metric_filter_in: {},
  default_metric_filter_not_in: {},
  default_metric_what: ['count_norm'],
  default_num_series: 5,
  disabled_v1: false,
  skip_error_code: [504, 502],
  skip_error_count: 10,
};

const meta = document.querySelector('meta[name="settings"]');
let metaSettings = defaultSettings;
if (meta !== null) {
  try {
    const serverConfig = { ...JSON.parse(meta.getAttribute('content')!) } as settings;
    metaSettings = {
      ...metaSettings,
      ...serverConfig,
      default_metric_filter_in: serverConfig.default_metric_filter_in
        ? normalizeFilterKey(serverConfig.default_metric_filter_in)
        : defaultSettings.default_metric_filter_in,
      default_metric_filter_not_in: serverConfig.default_metric_filter_not_in
        ? normalizeFilterKey(serverConfig.default_metric_filter_not_in)
        : defaultSettings.default_metric_filter_not_in,
      default_metric_what: serverConfig.default_metric_what.length
        ? serverConfig.default_metric_what
        : defaultSettings.default_metric_what,
      default_num_series: serverConfig.default_num_series
        ? serverConfig.default_num_series
        : defaultSettings.default_num_series,
    };
  } catch (e) {}
}

export const globalSettings: settings = metaSettings;
export const pxPerChar = 8;
export const maxTagsSize = 16; // max 32 or edit TAG_KEY enum

export const buildVersion = document.querySelector('meta[name="build-version"]')?.getAttribute('content') ?? null;
