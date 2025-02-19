// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { memo, useCallback, useEffect, useMemo, useState } from 'react';
import type { TagKey } from '@/api/enum';
import { Tooltip } from '@/components/UI';
import { produce } from 'immer';
import { sortEntity } from '@/common/helpers';
import cn from 'classnames';

import { setUpdatedTag, useVariableListStore } from '@/store2/variableList';
import type { QueryParams } from '@/url2';
import { useVariableLink } from '@/hooks/useVariableLink';
import { getTagDescription, getTagValue } from '@/view/utils2';
import { VariableControl } from '@/components/VariableControl';
import type { SelectOptionProps } from '@/components/Select';
import { formatTagValue } from '@/view/api';
import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';
import { useMetricMeta } from '@/hooks/useMetricMeta';
import { useMetricName } from '@/hooks/useMetricName';
import { type StatsHouseStore, useStatsHouse } from '@/store2';

export type PlotControlFilterTagProps = {
  tagKey: TagKey;
  className?: string;
};

const selectorStore = ({ params: { variables } }: StatsHouseStore) => variables;
const { setParams } = useStatsHouse.getState();

export const PlotControlFilterTag = memo(function PlotControlFilterTag({
  tagKey,
  className,
}: PlotControlFilterTagProps) {
  const variables = useStatsHouse(selectorStore);

  const {
    plot: { id, filterIn, filterNotIn, groupBy },
  } = useWidgetPlotContext();
  const meta = useMetricMeta(useMetricName(true));
  const filterInTag = filterIn[tagKey];
  const filterNotInTag = filterNotIn[tagKey];
  const groupByTag = groupBy.includes(tagKey);

  const variableInfo = useVariableLink(id, tagKey);
  const variable = (variableInfo?.variableKey && variables[variableInfo.variableKey]) || undefined;
  const tagList = useVariableListStore((s) => s.tags[id]?.[tagKey]);

  const [negativeTag, setNegativeTag] = useState<boolean>(!!filterNotInTag?.length);
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
            const pl = p.plots[id];
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
    [id, variableInfo]
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
            const pl = p.plots[id];
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
    [id, variableInfo]
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
      } else if (id) {
        setParams(
          produce((p) => {
            const pl = p.plots[id];
            if (pl) {
              if (negativeTag) {
                pl.filterNotIn[tagKey] = values;
                pl.filterIn[tagKey] = [];
              } else {
                pl.filterIn[tagKey] = values;
                pl.filterNotIn[tagKey] = [];
              }
            }
          })
        );
      }
    },
    [id, negativeTag, variableInfo]
  );
  const onSetUpdateTag = useCallback(
    (tagKey: TagKey | undefined, value: boolean) => {
      setUpdatedTag(id, tagKey, value);
    },
    [id]
  );

  const customValue = useMemo(
    () =>
      (!tagList?.list?.length || tagList?.more) &&
      ((v: string): SelectOptionProps => {
        const value = getTagValue(meta, tagKey, v);
        const tagMeta = meta?.tags?.[+tagKey];
        const name = formatTagValue(value, tagMeta?.value_comments?.[value], tagMeta?.raw, tagMeta?.raw_kind);
        return { value, name };
      }),
    [meta, tagKey, tagList?.list?.length, tagList?.more]
  );

  useEffect(() => {
    setNegativeTag(!!filterNotInTag?.length);
  }, [filterNotInTag?.length]);

  return (
    <VariableControl<TagKey>
      className={className}
      target={tagKey}
      placeholder={getTagDescription(meta, tagKey)}
      negative={variable?.negative ?? negativeTag ?? false}
      setNegative={onSetNegativeTag}
      groupBy={variable?.groupBy ?? groupByTag}
      setGroupBy={onSetGroupBy}
      values={(variable && !variable.negative ? variable.values : undefined) ?? filterInTag}
      notValues={(variable && variable.negative ? variable.values : undefined) ?? filterNotInTag}
      onChange={onFilterChange}
      tagMeta={tagList?.tagMeta ?? meta?.tagsObject[tagKey]}
      setOpen={onSetUpdateTag}
      list={tagList?.list}
      loaded={tagList?.loaded}
      more={tagList?.more}
      customValue={customValue}
      customBadge={
        variable && (
          <Tooltip<'span'>
            as="span"
            title={`is variable: ${variable?.description || variable?.name}`}
            className={cn(
              'input-group-text bg-transparent text-nowrap pt-0 pb-0 me-2',
              (variable?.negative ?? negativeTag) ? 'border-danger text-danger' : 'border-success text-success'
            )}
          >
            <span className="small">{variable?.name}</span>
          </Tooltip>
        )
      }
    />
  );
});
