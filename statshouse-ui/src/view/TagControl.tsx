// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { ChangeEvent, memo, useCallback, useId, useMemo, useState } from 'react';
import { formatTagValue, metricTag, whatToWhatDesc } from './api';
import useDeepCompareEffect from 'use-deep-compare-effect';
import { debug } from '../common/debug';
import { formatPercent, normalizeTagValues } from './utils';
import { Select, SelectOptionProps } from '../components';
import { ReactComponent as SVGSortAlphaDown } from 'bootstrap-icons/icons/sort-alpha-down.svg';
import { ReactComponent as SVGLayers } from 'bootstrap-icons/icons/layers.svg';
import {
  selectorLoadTagsList,
  selectorMetricsMeta,
  selectorParamsPlotsByIndex,
  selectorParamsTagSync,
  selectorSetPlotParamsTag,
  selectorSetPlotParamsTagGroupBy,
  selectorTagsListAbortControllerByPlotAndTag,
  selectorTagsListByPlotAndTag,
  selectorTagsListByPlotAndTagAllSync,
  selectorTagsListMoreByPlotAndTag,
  selectorTagsListSKeyByPlot,
  selectorTagsSKeyListAbortControllerByPlot,
  selectorTagsSKeyListMoreByPlot,
  selectorTimeRange,
  useStore,
} from '../store';
import cn from 'classnames';

export const TagControl = memo(function TagControl_(props: {
  tag: metricTag;
  indexTag: number;
  indexPlot: number;
  tagID: string;
  sync?: boolean;
  dashboard?: boolean;
  small?: boolean;
}) {
  const { tag, tagID, sync, indexTag, indexPlot, dashboard = false, small = false } = props;
  const id = useId();
  const [sortByCount, setSortByCount] = useState(true);
  const [focused, setFocused] = useState(false);
  const selectorParamsPlot = useMemo(() => selectorParamsPlotsByIndex.bind(undefined, indexPlot), [indexPlot]);
  const sel = useStore(selectorParamsPlot);
  const [posFilter, setPosFilter] = useState(!sel.filterNotIn[tagID]);
  const load = useStore(selectorLoadTagsList);
  const setPlotParamsTagByIndex = useStore(selectorSetPlotParamsTag);
  const setPlotParamsTagGroupByByIndex = useStore(selectorSetPlotParamsTagGroupBy);
  const timeRange = useStore(selectorTimeRange);
  const tagSync = useStore(selectorParamsTagSync);
  const metricsMeta = useStore(selectorMetricsMeta);

  const selectorTagMore = useMemo(
    () =>
      indexTag === -1
        ? selectorTagsSKeyListMoreByPlot.bind(undefined, indexPlot)
        : selectorTagsListMoreByPlotAndTag.bind(undefined, indexPlot, indexTag),
    [indexPlot, indexTag]
  );
  const more = useStore(selectorTagMore);

  const selectorTagsList = useMemo(
    () =>
      indexTag === -1
        ? selectorTagsListSKeyByPlot.bind(undefined, indexPlot)
        : dashboard
        ? selectorTagsListByPlotAndTagAllSync.bind(undefined, indexPlot, indexTag)
        : selectorTagsListByPlotAndTag.bind(undefined, indexPlot, indexTag),
    [dashboard, indexPlot, indexTag]
  );
  const list = useStore(selectorTagsList);
  const selectorTagsListAbortController = useMemo(
    () =>
      indexTag === -1
        ? selectorTagsSKeyListAbortControllerByPlot.bind(undefined, indexPlot)
        : selectorTagsListAbortControllerByPlotAndTag.bind(undefined, indexPlot, indexTag),
    [indexPlot, indexTag]
  );

  const loader = useStore(selectorTagsListAbortController);
  const listSort = useMemo<SelectOptionProps[]>(
    () =>
      normalizeTagValues(list, sortByCount).map((v) => {
        const name = formatTagValue(v.value, tag?.value_comments?.[v.value], tag.raw, tag.raw_kind);
        const percent = formatPercent(v.count);
        const title = tag?.value_comments?.[v.value]
          ? `${name} (${formatTagValue(v.value, undefined, tag.raw, tag.raw_kind)}): ${percent}`
          : `${name}: ${percent}`;
        return {
          name: name,
          html: `<div class="d-flex"><div class="flex-grow-1 me-2 overflow-hidden text-nowrap">${name}</div><div class="text-end">${percent}</div></div>`,
          value: v.value,
          title: title,
        };
      }),
    [list, sortByCount, tag.raw, tag.raw_kind, tag?.value_comments]
  );

  const otherFilterIn = { ...sel.filterIn };
  delete otherFilterIn[tagID];
  const otherFilterNotIn = { ...sel.filterNotIn };
  delete otherFilterNotIn[tagID];
  const syncTitle = useMemo(() => {
    if (sync) {
      const group = tagSync.find((group) => group[indexPlot] === indexTag);
      if (group) {
        return group
          .map((tag) => {
            if (tag !== null) {
              const tagInfo = metricsMeta[sel.metricName]?.tags?.[tag];
              const description = tagInfo?.description
                ? tagInfo.description
                : tagInfo?.name
                ? tagInfo.name
                : `tag${tag}`;
              const namePlot = `${sel.metricName}: ${sel.what.map((qw) => whatToWhatDesc(qw)).join(',')}`;
              return `${namePlot} - ${description}`;
            }
            return null;
          })
          .filter(Boolean)
          .join('\n');
      }
    }
    return '';
  }, [indexPlot, indexTag, metricsMeta, sel.metricName, sel.what, sync, tagSync]);
  // load tag values
  useDeepCompareEffect(() => {
    if (!focused) {
      return;
    }
    debug.log('fetching tag values for', sel.metricName, tagID, otherFilterIn, otherFilterNotIn);
    const group = tagSync.find((group) => group[indexPlot] === indexTag);
    if (group) {
      group.forEach((tag, plot) => {
        if (tag !== null) {
          load(plot, tag);
        }
      });
    } else {
      load(indexPlot, indexTag);
    }
  }, [
    load,
    indexPlot,
    indexTag,
    sortByCount,
    focused,
    sel.useV2,
    tagID,
    sel.metricName,
    timeRange.from,
    timeRange.to,
    sel.what,
    otherFilterIn,
    otherFilterNotIn,
    tagSync,
  ]);

  const selectValues = useMemo(
    () =>
      [...(sel.filterIn[tagID] ?? []), ...(sel.filterNotIn[tagID] ?? [])].map((t) => {
        if (t[0] !== ' ' && tag.raw) {
          return Object.entries(tag.value_comments ?? {}).find(([, tag_comment]) => tag_comment === t)?.[0] ?? t;
        }
        return t;
      }),
    [sel.filterIn, sel.filterNotIn, tag, tagID]
  );

  const onSortOrderToggle = useCallback(() => {
    setSortByCount((s) => !s);
  }, []);

  const onExpandChange = useCallback(
    (e: ChangeEvent<HTMLInputElement>) => {
      setPlotParamsTagGroupByByIndex(indexPlot, tagID, e.target.checked);
    },
    [indexPlot, setPlotParamsTagGroupByByIndex, tagID]
  );

  const onNegateChange = useCallback(
    (e: ChangeEvent<HTMLInputElement>) => {
      setPosFilter(!e.target.checked);
      setPlotParamsTagByIndex(indexPlot, tagID, (s) => s, !e.target.checked);
    },
    [indexPlot, setPlotParamsTagByIndex, tagID]
  );

  // add filter
  const onFilterChange = useCallback(
    (val?: string | string[]) => {
      debug.log(`add ${posFilter ? 'positive' : 'negative'} filter for`, tagID, val);
      const newValues = Array.isArray(val) ? val : val ? [val] : [];
      setPlotParamsTagByIndex(indexPlot, tagID, newValues, posFilter);
    },
    [posFilter, tagID, setPlotParamsTagByIndex, indexPlot]
  );

  // remove filter
  const onValueClick = useCallback(
    (e: React.MouseEvent<HTMLButtonElement>) => {
      const val = (e.target as HTMLButtonElement).value;
      debug.log('remove filter for', tagID, val);
      setPlotParamsTagByIndex(
        indexPlot,
        tagID,
        (s) => s.filter((v) => v !== val),
        (s) => s
      );
    },
    [tagID, setPlotParamsTagByIndex, indexPlot]
  );

  const onTagSelectFocus = useCallback(() => {
    setFocused(true);
  }, [setFocused]);

  const onTagSelectBlur = useCallback(() => {
    setFocused(false);
  }, [setFocused]);

  const description = tag.description
    ? tag.description
    : tag.name
    ? tag.name
    : indexTag > -1
    ? `tag ${indexTag}`
    : 'tag _s';

  return (
    <div className="mb-3">
      <div className="d-flex align-items-baseline">
        <div className={cn('input-group flex-nowrap w-100 me-4 align-items-baseline', small && 'input-group-sm')}>
          <Select
            value={selectValues}
            className="sh-select form-control"
            classNameList="dropdown-menu"
            placeholder={description}
            loading={!!loader}
            showSelected={false}
            onceSelectByClick
            multiple
            moreItems={more}
            showCountItems
            onChange={onFilterChange}
            options={listSort}
            onFocus={onTagSelectFocus}
            onBlur={onTagSelectBlur}
          />
          <input
            type="checkbox"
            className="btn-check"
            id={`toggle-negate-${tagID}-${id}`}
            autoComplete="off"
            checked={!posFilter}
            onChange={onNegateChange}
          />
          <label
            className="btn btn-outline-primary"
            htmlFor={`toggle-negate-${tagID}-${id}`}
            title="Negate next selection"
          >
            −
          </label>
          <input
            type="checkbox"
            className="btn-check"
            id={`sortByAlpha-${tagID}-${id}`}
            autoComplete="off"
            checked={!sortByCount}
            onChange={onSortOrderToggle}
          />
          <label className="btn btn-outline-primary" htmlFor={`sortByAlpha-${tagID}-${id}`} title="Sort alphabetically">
            <SVGSortAlphaDown />
          </label>
        </div>
        <div className="form-check form-switch flex-shrink-0">
          <input
            className="form-check-input"
            type="checkbox"
            value=""
            id={`group-by-${tagID}-${id}`}
            checked={sel.groupBy.indexOf(tagID) >= 0}
            onChange={onExpandChange}
          />
          <label className="form-check-label" htmlFor={`group-by-${tagID}-${id}`} title="Group by">
            <SVGLayers />
          </label>
        </div>
      </div>
      <div className="d-flex flex-wrap">
        {sync && (
          <span
            title={syncTitle}
            className="input-group-text border-success bg-transparent text-success text-nowrap pt-0 pb-0 mt-2 me-2 "
          >
            <span className="small">synced</span>
          </span>
        )}
        {(sel.filterIn[tagID] || []).map((v) => (
          <button
            type="button"
            key={v}
            value={v}
            className="overflow-force-wrap btn btn-sm pt-0 pb-0 mt-2 me-2 btn-success"
            style={{ userSelect: 'text' }}
            onClick={onValueClick}
          >
            {formatTagValue(v, tag?.value_comments?.[v], tag.raw, tag.raw_kind)}
          </button>
        ))}
        {(sel.filterNotIn[tagID] || []).map((v) => (
          <button
            key={v}
            value={v}
            className="overflow-force-wrap btn btn-sm pt-0 pb-0 mt-2 me-2 btn-danger"
            style={{ userSelect: 'text' }}
            onClick={onValueClick}
          >
            {formatTagValue(v, tag?.value_comments?.[v], tag.raw, tag.raw_kind)}
          </button>
        ))}
      </div>
    </div>
  );
});
