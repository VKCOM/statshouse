// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo, useCallback, useState } from 'react';
import { type TagKey } from 'api/enum';
import { Tooltip } from 'components/UI';
import { produce } from 'immer';
import { sortEntity } from 'common/helpers';
import cn from 'classnames';

import { setUpdatedTag, useVariableListStore } from 'store2/variableList';
import { type PlotKey, QueryParams } from 'url2';
import { useStatsHouseShallow } from 'store2';
import { useVariableLink } from 'hooks/useVariableLink';
import { getTagDescription } from 'view/utils2';
import { VariableControl } from '../../../components/VariableControl';

export type PlotControlFilterTagProps = {
  plotKey: PlotKey;
  tagKey: TagKey;
  className?: string;
};

export function _PlotControlFilterTag({ plotKey, tagKey, className }: PlotControlFilterTagProps) {
  const { filterIn, filterNotIn, groupBy, variables, meta, setParams } = useStatsHouseShallow((s) => ({
    // metricName: s.params.plots[plotKey]?.metricName ?? '',
    filterIn: s.params.plots[plotKey]?.filterIn[tagKey],
    filterNotIn: s.params.plots[plotKey]?.filterNotIn[tagKey],
    groupBy: s.params.plots[plotKey]?.groupBy.includes(tagKey),
    variables: s.params.variables,
    // variableInfo: s.plotVariablesLink[plotKey]?.[tagKey],
    meta: s.metricMeta[s.params.plots[plotKey]?.metricName ?? ''],
    setParams: s.setParams,
  }));

  const variableInfo = useVariableLink(plotKey, tagKey);
  const variable = (variableInfo?.variableKey && variables[variableInfo.variableKey]) || undefined;
  // const meta = useMetricsStore((s) => s.meta[metricName]);
  const tagList = useVariableListStore((s) => s.tags[plotKey]?.[tagKey]);

  const [negativeTag, setNegativeTag] = useState<boolean>((filterNotIn?.length ?? 0) > 0);
  const onSetNegativeTag = useCallback(
    (tagKey: TagKey | undefined, value: boolean) => {
      if (tagKey == null) {
        return;
      }
      if (variableInfo) {
        setParams(
          produce((p) => {
            const v = p.variables[variableInfo.variableKey];
            if (v) {
              v.negative = value;
            }
          })
        );
      } else {
        setNegativeTag(value);
        setParams(
          produce((p) => {
            const pl = p.plots[plotKey];
            if (pl) {
              if (value) {
                const tags = pl.filterIn[tagKey];
                delete pl.filterIn[tagKey];
                pl.filterNotIn[tagKey] = tags ?? [];
              } else {
                const tags = pl.filterNotIn[tagKey];
                delete pl.filterNotIn[tagKey];
                pl.filterIn[tagKey] = tags ?? [];
              }
            }
          })
        );
      }
    },
    [plotKey, setParams, variableInfo]
  );

  const onSetGroupBy = useCallback(
    (tagKey: TagKey | undefined, value: boolean) => {
      if (tagKey == null) {
        return;
      }
      if (variableInfo) {
        setParams(
          produce((p) => {
            const v = p.variables[variableInfo.variableKey];
            if (v) {
              v.groupBy = value;
            }
          })
        );
      } else {
        setParams(
          produce<QueryParams>((p) => {
            const pl = p.plots[plotKey];
            if (pl) {
              if (value) {
                pl.groupBy = sortEntity([...pl.groupBy, tagKey]);
              } else {
                pl.groupBy = pl.groupBy.filter((gb) => gb !== tagKey);
              }
            }
          })
        );
      }
    },
    [plotKey, setParams, variableInfo]
  );

  const onFilterChange = useCallback(
    (tagKey: TagKey | undefined, values: string[]) => {
      if (tagKey == null) {
        return;
      }
      if (variableInfo) {
        setParams(
          produce((p) => {
            const v = p.variables[variableInfo.variableKey];
            if (v) {
              v.values = values;
            }
          })
        );
      } else if (plotKey) {
        setParams(
          produce((p) => {
            const pl = p.plots[plotKey];
            if (pl) {
              if (negativeTag) {
                pl.filterNotIn[tagKey] = values;
              } else {
                pl.filterIn[tagKey] = values;
              }
            }
          })
        );
      }
    },
    [negativeTag, plotKey, setParams, variableInfo]
  );
  const onSetUpdateTag = useCallback(
    (tagKey: TagKey | undefined, value: boolean) => {
      setUpdatedTag(plotKey, tagKey, value);
    },
    [plotKey]
  );

  return (
    <VariableControl<TagKey>
      className={className}
      target={tagKey}
      placeholder={getTagDescription(meta, tagKey)}
      negative={variable?.negative ?? negativeTag ?? false}
      setNegative={onSetNegativeTag}
      groupBy={variable?.groupBy ?? groupBy}
      setGroupBy={onSetGroupBy}
      values={(variable && !variable.negative ? variable.values : undefined) ?? filterIn}
      notValues={(variable && variable.negative ? variable.values : undefined) ?? filterNotIn}
      onChange={onFilterChange}
      tagMeta={tagList?.tagMeta ?? meta?.tagsObject[tagKey]}
      setOpen={onSetUpdateTag}
      list={tagList?.list}
      loaded={tagList?.loaded}
      more={tagList?.more}
      customValue={tagList?.more}
      customBadge={
        variable && (
          <Tooltip<'span'>
            as="span"
            title={`is variable: ${variable?.description || variable?.name}`}
            className={cn(
              'input-group-text bg-transparent text-nowrap pt-0 pb-0 me-2',
              variable?.negative ?? negativeTag ? 'border-danger text-danger' : 'border-success text-success'
            )}
          >
            <span className="small">{variable?.name}</span>
          </Tooltip>
        )
      }
    />
  );
}
export const PlotControlFilterTag = memo(_PlotControlFilterTag);
