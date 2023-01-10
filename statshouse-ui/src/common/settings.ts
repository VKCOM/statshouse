// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { queryWhat } from '../view/api';

export interface settings {
  readonly vkuth_app_name?: string;
  readonly default_metric: string;
  readonly default_metric_group_by: readonly string[];
  readonly default_metric_filter_in: Readonly<Record<string, string[]>>;
  readonly default_metric_filter_not_in: Readonly<Record<string, string[]>>;
  readonly default_metric_what: readonly queryWhat[];
  readonly disabled_v1: boolean;
}

const defaultSettings: settings = {
  vkuth_app_name: '',
  default_metric: '__agg_bucket_receive_delay_sec',
  default_metric_group_by: [],
  default_metric_filter_in: {},
  default_metric_filter_not_in: {},
  default_metric_what: ['count_norm'],
  disabled_v1: false,
};

const meta = document.querySelector('meta[name="settings"]');
let metaSettings = defaultSettings;
if (meta !== null) {
  try {
    metaSettings = { ...metaSettings, ...JSON.parse(meta.getAttribute('content')!) } as settings;
  } catch (e) {}
}

export const globalSettings: settings = metaSettings!;
export const pxPerChar = 8;

export const buildVersion = document.querySelector('meta[name="build-version"]')?.getAttribute('content') ?? null;
