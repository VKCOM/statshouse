// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';
import { StatsHouseStore, useStatsHouseShallow } from '@/store2';
import { ApiQuery, useApiQuery } from '@/api/query';
import { useMemo } from 'react';
import type { SelectOptionProps } from '@/components/Select';
import { formatPercent } from '@/view/utils2';

const selectorStore = ({ params: { timeRange, timeShifts, variables } }: StatsHouseStore) => ({
  timeRange,
  timeShifts,
  variables,
});

const selectorQuery = (response?: ApiQuery) => {
  if (response) {
    return response.data.series.series_meta;
  }
  return undefined;
};

export function useMaxHosts(visible: boolean = false, priority: number = 2) {
  const { plot } = useWidgetPlotContext();
  const { timeRange, timeShifts, variables } = useStatsHouseShallow(selectorStore);
  const queryData = useApiQuery(plot, timeRange, timeShifts, variables, selectorQuery, visible, priority);
  const series_meta = queryData.data;

  return useMemo<
    [
      SelectOptionProps[][],
      { top_max_host: string; top_max_host_percent: string; max_hosts: { host: string; percent: string }[] }[],
    ]
  >(() => {
    const maxHostLists: SelectOptionProps[][] = new Array(series_meta?.length ?? 0).fill([]);
    const maxHostValues =
      series_meta?.map((meta, indexMeta) => {
        const max_host_map =
          meta.max_hosts?.reduce(
            (res, host) => {
              if (host) {
                res[host] = (res[host] ?? 0) + 1;
              }
              return res;
            },
            {} as Record<string, number>
          ) ?? {};
        const max_host_total = meta.max_hosts?.filter(Boolean).length ?? 1;

        const max_hosts =
          meta.max_hosts?.map((host) => {
            const percent =
              meta.max_hosts !== null && max_host_map && host
                ? formatPercent((max_host_map[host] ?? 0) / max_host_total)
                : '';
            return { host, percent };
          }) ?? [];
        maxHostLists[indexMeta] = Object.entries(max_host_map)
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
        return {
          top_max_host: maxHostLists[indexMeta]?.[0]?.value ?? '',
          top_max_host_percent: maxHostLists[indexMeta]?.[0]?.title ?? '',
          max_hosts,
        };
      }) ?? [];
    return [maxHostLists, maxHostValues];
  }, [series_meta]);
}
