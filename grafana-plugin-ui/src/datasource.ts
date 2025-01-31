// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { DataSourceInstanceSettings, dateTimeAsMoment, MetricFindValue, ScopedVars } from '@grafana/data';
import { DataSourceWithBackend, getTemplateSrv } from '@grafana/runtime';
import {
  KeysMap,
  MetricNamesResponse,
  MetricPropertiesResponse,
  MetricTagValuesResponse,
  SHDataSourceOptions,
  SHQuery,
  TagValue,
  VariableQuery,
} from './types';

export class DataSource extends DataSourceWithBackend<SHQuery, SHDataSourceOptions> {
  constructor(instanceSettings: DataSourceInstanceSettings<SHDataSourceOptions>) {
    super(instanceSettings);
  }

  applyTemplateVariables(query: SHQuery, scopedVars: ScopedVars): Record<string, any> {
    const km: KeysMap = {};
    for (let i in query.keys) {
      km[i] = query.keys[i];
      km[i].values = query.keys[i].values.map((value: string) => {
        return getTemplateSrv().replace(value, scopedVars);
      });
    }

    return {
      ...query,
      url: getTemplateSrv().replace(query.url, scopedVars),
      keys: km,
    };
  }

  getAvailableMetricNames(): Promise<MetricNamesResponse> {
    return this.getResource('metric-names');
  }

  getMetricProperties(metricName: string): Promise<MetricPropertiesResponse> {
    return this.getResource('metric', { metric_name: metricName });
  }

  getMetricTagValues(
    metricName: string,
    what: string[],
    tagID: string,
    filters: string[],
    timeFrom: number,
    timeTo: number
  ): Promise<MetricTagValuesResponse> {
    return this.getResource('metric-tag-values', {
      metric_name: metricName,
      what: what,
      tag_id: tagID,
      filters: filters,
      time_from: timeFrom,
      time_to: timeTo,
    });
  }

  metricFindQuery(query: VariableQuery, options?: any): Promise<MetricFindValue[]> {
    return this.getResource('metric-tag-values', {
      query: getTemplateSrv().replace(query.url),
      time_from: dateTimeAsMoment(options.range.from).unix(),
      time_to: dateTimeAsMoment(options.range.to).unix(),
    }).then((data: MetricTagValuesResponse) => {
      return data.tag_values.map((value: TagValue) => ({ text: value.value }));
    });
  }
}
