// Copyright 2026 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { Button } from '@/components/UI';
import { ReactComponent as SVGSearch } from 'bootstrap-icons/icons/search.svg';
import { Dropdown } from '@/components/UI/Dropdown';
import { useStatsHouseShallow } from '@/store2';
import { useEffect, useMemo, useState } from 'react';
import { findPair, getMetricsMeta, type VariableMetricPair } from '@/store2/urlStore/getAutoSearchVariable';
import type { MetricMeta } from '@/store2/metricsMetaStore';
import cn from 'classnames';
import { isPromQL } from '@/store2/helpers';
import type { VariableKey, VariableParams } from '@/url2';
export type DashboardVariableFindButtonProps = {
  onAddVariable?: (value: VariableMetricPair) => void;
  variables?: Partial<Record<VariableKey, VariableParams>>;
};
export function DashboardVariableFindButton({
  onAddVariable,
  variables: localVariables,
}: DashboardVariableFindButtonProps) {
  const { plots, variables } = useStatsHouseShallow(({ params: { plots, variables } }) => ({ plots, variables }));
  const [metricsMeta, setMetricsMeta] = useState<MetricMeta[]>([]);
  const metricsCountMap = useMemo(
    () =>
      Object.values(plots).reduce(
        (res, plot) => {
          if (plot && !isPromQL(plot)) {
            res[plot.metricName] ??= 0;
            res[plot.metricName]++;
          }
          return res;
        },
        {} as Record<string, number>
      ),
    [plots]
  );

  useEffect(() => {
    getMetricsMeta(Object.keys(metricsCountMap)).then(setMetricsMeta);
  }, [metricsCountMap]);

  const variablesName = useMemo(
    () => Object.values(localVariables ?? variables).map((v) => v?.name),
    [localVariables, variables]
  );
  const pair = useMemo(
    () =>
      findPair(metricsMeta)
        .filter((p) => !variablesName.includes(p.name))
        .map((p) => ({
          ...p,
          count: p.links.reduce((res, { metricName }) => res + (metricsCountMap[metricName] ?? 0), 0),
        })),
    [metricsCountMap, metricsMeta, variablesName]
  );

  return (
    <Dropdown
      className={cn('btn btn-outline-primary', !pair.length && 'disabled')}
      caption={
        <>
          <SVGSearch />
          &nbsp;Auto&nbsp;variable
        </>
      }
    >
      <div className="list-group text-nowrap shadow shadow-1">
        {pair.map((item) => (
          <Button
            key={item.name}
            type="button"
            className="list-group-item list-group-item-action d-flex gap-2 align-items-center"
            onClick={() => {
              onAddVariable?.(item);
            }}
            title={item.links
              .map(
                (l) =>
                  `${l.metricName}${metricsCountMap[l.metricName] > 1 ? ` [${metricsCountMap[l.metricName]}]` : ''}`
              )
              .join('\n')}
          >
            {item.name} [{item.count}]
          </Button>
        ))}
      </div>
    </Dropdown>
  );
}
