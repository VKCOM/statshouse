// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useMemo, useState } from 'react';
import cn from 'classnames';
import { TagSelect } from '../TagSelect';
import { formatTagValue } from '@/view/api';
import { SelectOptionProps } from '@/components/Select';
import { normalizeTagValues } from '@/view/utils';
import { MetricMetaTag } from '@/api/metric';
import { MetricTagValueInfo } from '@/api/metricTagValues';
import { escapeHTML } from '@/common/helpers';
import { Button } from '@/components/UI';
import { clearOuterInfo, formatPercent, isOuterInfo } from '@/view/utils2';
import { TooltipMarkdown } from '@/components/Markdown/TooltipMarkdown';

const emptyListArray: MetricTagValueInfo[] = [];
const emptyValues: string[] = [];

export type VariableControlProps<T> = {
  target?: T;
  placeholder?: string;
  negative?: boolean;
  setNegative?: (name: T | undefined, value: boolean) => void;
  groupBy?: boolean;
  setGroupBy?: (name: T | undefined, value: boolean) => void;
  className?: string;
  values?: string[];
  notValues?: string[];
  onChange?: (name: T | undefined, value: string[]) => void;
  tagMeta?: MetricMetaTag;
  more?: boolean;
  customValue?: boolean | ((value: string) => SelectOptionProps);
  loaded?: boolean;
  list?: MetricTagValueInfo[];
  small?: boolean;
  setOpen?: (name: T | undefined, value: boolean) => void;
  customBadge?: React.ReactNode;
};
export function VariableControl<T extends string>({
  target,
  placeholder,
  className,
  negative = false,
  setNegative,
  groupBy,
  setGroupBy,
  values = emptyValues,
  notValues = emptyValues,
  onChange,
  list = emptyListArray,
  loaded,
  more,
  customValue,
  tagMeta,
  small,
  setOpen,
  customBadge,
}: VariableControlProps<T>) {
  const [sortByName, setSortByName] = useState(false);

  const listSort = useMemo<SelectOptionProps[]>(
    () =>
      normalizeTagValues(list, !sortByName).map((v) => {
        const name = formatTagValue(v.value, tagMeta?.value_comments?.[v.value], tagMeta?.raw, tagMeta?.raw_kind);
        const percent = formatPercent(v.count);
        const title = tagMeta?.value_comments?.[v.value]
          ? `${name} (${formatTagValue(v.value, undefined, tagMeta?.raw, tagMeta?.raw_kind)}): ${percent}`
          : `${name}: ${percent}`;

        return {
          name: name,
          html: `<div class="d-flex"><div class="flex-grow-1 me-2 overflow-hidden text-nowrap">${escapeHTML(
            name
          )}</div><div class="text-end">${escapeHTML(percent)}</div></div>`,
          value: v.value,
          title: title,
        };
      }),
    [list, sortByName, tagMeta]
  );

  const onSelectFocus = useCallback(() => {
    setOpen?.(target, true);
  }, [setOpen, target]);

  const onSelectBlur = useCallback(() => {
    setOpen?.(target, false);
  }, [setOpen, target]);

  const onChangeFilter = useCallback(
    (value?: string | string[] | undefined) => {
      const v = value == null ? [] : Array.isArray(value) ? value : [value];
      onChange?.(target, v);
    },
    [target, onChange]
  );
  const onSetNegative = useCallback(
    (value: boolean) => {
      setNegative?.(target, value);
    },
    [target, setNegative]
  );
  const onSetGroupBy = useCallback(
    (value: boolean) => {
      setGroupBy?.(target, value);
    },
    [target, setGroupBy]
  );
  const onRemoveFilter = useCallback<React.MouseEventHandler<HTMLButtonElement>>(
    (event) => {
      const value = event.currentTarget.getAttribute('data-value');
      const oldValues = negative ? notValues : values;
      onChange?.(
        target,
        oldValues.filter((v) => v !== value)
      );
    },
    [negative, notValues, values, onChange, target]
  );
  return (
    <div className={className}>
      <div className="d-flex align-items-center">
        <div className={cn('input-group flex-nowrap w-100', small ? 'input-group-sm' : 'input-group')}>
          <TagSelect
            values={negative ? notValues : values}
            placeholder={clearOuterInfo(placeholder)}
            loading={loaded}
            onChange={onChangeFilter}
            moreOption={more}
            customValue={customValue}
            options={listSort}
            onFocus={onSelectFocus}
            onBlur={onSelectBlur}
            negative={negative}
            setNegative={onSetNegative}
            sort={sortByName}
            setSort={setSortByName}
            groupBy={groupBy}
            setGroupBy={onSetGroupBy}
          />
        </div>
      </div>
      <div className={cn('d-flex flex-wrap gap-2', (!!customBadge || values.length > 0 || notValues.length) && 'mt-2')}>
        {customBadge}
        {values.map((v) => (
          <Button
            key={v}
            type="button"
            data-value={v}
            className="overflow-force-wrap btn btn-sm py-0 btn-success"
            style={{ userSelect: 'text' }}
            onClick={onRemoveFilter}
            title={
              isOuterInfo(placeholder) ? (
                <div className="small text-secondary overflow-auto">
                  <TooltipMarkdown
                    description={placeholder}
                    value={formatTagValue(v, tagMeta?.value_comments?.[v], tagMeta?.raw, tagMeta?.raw_kind)}
                  />
                </div>
              ) : undefined
            }
            hover
          >
            {formatTagValue(v, tagMeta?.value_comments?.[v], tagMeta?.raw, tagMeta?.raw_kind)}
          </Button>
        ))}
        {notValues.map((v) => (
          <Button
            key={v}
            type="button"
            data-value={v}
            className="overflow-force-wrap btn btn-sm py-0 btn-danger"
            style={{ userSelect: 'text' }}
            onClick={onRemoveFilter}
            title={
              isOuterInfo(placeholder) ? (
                <div className="small text-secondary overflow-auto">
                  <TooltipMarkdown
                    description={placeholder}
                    value={formatTagValue(v, tagMeta?.value_comments?.[v], tagMeta?.raw, tagMeta?.raw_kind)}
                  />
                </div>
              ) : undefined
            }
            hover
          >
            {formatTagValue(v, tagMeta?.value_comments?.[v], tagMeta?.raw, tagMeta?.raw_kind)}
          </Button>
        ))}
      </div>
    </div>
  );
}
