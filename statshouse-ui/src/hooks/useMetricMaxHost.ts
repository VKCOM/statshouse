// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useMemo } from 'react';
import { ApiQuery, useApiQuery } from '@/api/query';
import { formatPercent } from '@/view/utils2';
import { useWidgetParamsContext, useWidgetPlotContext } from '@/contexts';

const maxHostSelector = (res?: ApiQuery) => res?.data?.series.series_meta;

export function useMetricMaxHost() {
  const { plot } = useWidgetPlotContext();
  const { params } = useWidgetParamsContext();
  const queryData = useApiQuery(plot, params, maxHostSelector);
  const response = queryData.data;
  const maxHosts = useMemo<(string[] | null)[]>(() => {
    if (!response) {
      return [];
    }
    return response.map((series_meta) => series_meta.max_hosts);
  }, [response]);
  const maxHostsMaxLength = useMemo(
    () =>
      maxHosts?.reduce(
        (res, hosts) => Math.max(res, hosts?.reduce((res_hosts, host) => Math.max(res_hosts, host.length), 0) ?? 0),
        0
      ),
    [maxHosts]
  );
  const maxHostsLists = useMemo(
    () =>
      maxHosts.map((hosts) => {
        const max_host_map =
          hosts?.reduce(
            (res, host) => {
              if (host) {
                res[host] = (res[host] ?? 0) + 1;
              }
              return res;
            },
            {} as Record<string, number>
          ) ?? {};
        const max_host_total = hosts?.filter(Boolean).length ?? 1;
        return Object.entries(max_host_map)
          .sort(([k, a], [n, b]) => (a > b ? -1 : a < b ? 1 : k > n ? 1 : k < n ? -1 : 0))
          .map(([host, count]) => {
            const percent = formatPercent(count / max_host_total);
            return {
              value: host,
              title: `${host}: ${percent}`,
              name: `${host}: ${percent}`,
              html: `<div class="d-flex"><div class="flex-grow-1 me-2 overflow-hidden text-nowrap">${host}</div><div class="text-end">${percent}</div></div>`,
            };
          });
      }),
    [maxHosts]
  );
  return useMemo(() => ({ maxHostsLists, maxHostsMaxLength }), [maxHostsLists, maxHostsMaxLength]);
}
