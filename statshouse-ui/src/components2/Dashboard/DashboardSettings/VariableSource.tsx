// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useEffect, useId, useMemo, useRef, useState } from 'react';
import { isTagKey, TAG_KEY, TagKey, toTagKey } from '@/api/enum';

import { ReactComponent as SVGTrash } from 'bootstrap-icons/icons/trash.svg';
import { ReactComponent as SVGPencil } from 'bootstrap-icons/icons/pencil.svg';
import cn from 'classnames';
import { produce } from 'immer';
import { TagBadges } from './TagBadges';
import { dequal } from 'dequal/lite';
import { mergeLeft } from '@/common/helpers';
import { FilterTag, VariableParamsSource, VariableSourceKey } from '@/url2';
import { setUpdatedSource, useVariableListStore, VariableItem } from '@/store2/variableList';
import { getTagDescription, isTagEnabled } from '@/view/utils2';
import { Button, ToggleButton } from '@/components/UI';
import { SelectMetric } from '@/components/SelectMertic';
import { VariableControl } from '@/components/VariableControl';
import { useMetricMeta } from '@/hooks/useMetricMeta';
import { useMetricName } from '@/hooks/useMetricName';

export type VariableSourceProps = {
  value?: VariableParamsSource;
  valueKey?: VariableSourceKey;
  onChange?: (valueKey: VariableSourceKey, value?: VariableParamsSource) => void;
};
export function VariableSource({ value, valueKey = '0', onChange }: VariableSourceProps) {
  const uid = useId();
  const [localMetric, setLocalMetric] = useState(value?.metric);
  const [localTag, setLocalTag] = useState(value?.tag);
  const [localFilterIn, setLocalFilterIn] = useState<FilterTag>(value?.filterIn ?? {});
  const [localFilterNotIn, setLocalFilterNotIn] = useState<FilterTag>(value?.filterNotIn ?? {});
  const [open, setOpen] = useState(!value?.metric);
  const listTags = useVariableListStore<Partial<Record<TagKey, VariableItem>>>(
    ({ source }) => (localMetric && source[localMetric]) ?? {}
  );

  const meta = useMetricMeta(useMetricName(true));

  const prevValue = useRef(value);

  const negativeTags = useMemo(() => {
    const n: Partial<Record<TagKey, boolean>> = {};
    Object.keys(localFilterIn).forEach((k) => {
      if (isTagKey(k)) {
        n[k] = false;
      }
    });
    Object.keys(localFilterNotIn).forEach((k) => {
      if (isTagKey(k)) {
        n[k] = true;
      }
    });

    return n;
  }, [localFilterIn, localFilterNotIn]);
  const onOpenToggle = useCallback(() => {
    setOpen((s) => !s);
  }, []);

  const onChangeMetric = useCallback((metric?: string | string[]) => {
    if (Array.isArray(metric)) {
      setLocalMetric(metric[0]);
    } else {
      setLocalMetric(metric);
    }
  }, []);

  const onRadioTag = useCallback((event: React.ChangeEvent<HTMLInputElement>) => {
    const value = toTagKey(event.currentTarget.value) ?? undefined;
    setLocalTag(value);
  }, []);

  const onRemove = useCallback(() => {
    onChange?.(valueKey, undefined);
  }, [valueKey, onChange]);

  const onSetNegativeTag = useCallback(
    (tagKey: TagKey | undefined, value: boolean) => {
      if (tagKey == null) {
        return;
      }
      if (value) {
        setLocalFilterNotIn(
          produce((f) => {
            f[tagKey] = localFilterIn[tagKey]?.slice() ?? [];
          })
        );
        setLocalFilterIn(
          produce((f) => {
            delete f[tagKey];
          })
        );
      } else {
        setLocalFilterIn(
          produce((f) => {
            f[tagKey] = localFilterNotIn[tagKey]?.slice() ?? [];
          })
        );
        setLocalFilterNotIn(
          produce((f) => {
            delete f[tagKey];
          })
        );
      }
    },
    [localFilterIn, localFilterNotIn]
  );

  const onFilterChange = useCallback(
    (tagKey: TagKey | undefined, values: string[]) => {
      if (tagKey == null) {
        return;
      }

      const negative = negativeTags[tagKey];
      if (negative) {
        setLocalFilterNotIn(
          produce((f) => {
            f[tagKey] = values;
          })
        );
      } else {
        setLocalFilterIn(
          produce((f) => {
            f[tagKey] = values;
          })
        );
      }
    },
    [negativeTags]
  );

  const onSetUpdateTag = useCallback(
    (tagKey: TagKey | undefined, updated: boolean) => {
      if (localMetric && tagKey) {
        const filterNotIn = { ...localFilterNotIn };
        delete filterNotIn[tagKey];
        const filterIn = { ...localFilterIn };
        delete filterIn[tagKey];
        setUpdatedSource({ id: valueKey, metric: localMetric, tag: tagKey, filterNotIn, filterIn }, updated);
      }
    },
    [localFilterIn, localFilterNotIn, localMetric, valueKey]
  );
  const placeholder = useMemo(() => {
    if (localMetric && localTag) {
      return localMetric + ': ' + getTagDescription(meta, localTag);
    }
    return '';
  }, [localMetric, localTag, meta]);

  useEffect(() => {
    if (!localMetric) {
      setLocalTag(undefined);
    } else {
      setLocalTag((t) => (t != null ? t : TAG_KEY._0));
    }
  }, [localMetric]);

  useEffect(() => {
    if (dequal(prevValue.current, value)) {
      setLocalFilterIn((f) => mergeLeft(f, value?.filterIn ?? {}));
      setLocalFilterNotIn((f) => mergeLeft(f, value?.filterNotIn ?? {}));
      setLocalMetric(value?.metric);
      setLocalTag(value?.tag ?? TAG_KEY._0);
      prevValue.current = value;
    }
  }, [value]);

  useEffect(() => {
    if (localMetric && localTag) {
      onChange?.(valueKey, {
        id: valueKey,
        metric: localMetric,
        tag: localTag,
        filterIn: localFilterIn,
        filterNotIn: localFilterNotIn,
      });
    }
  }, [valueKey, localFilterIn, localFilterNotIn, localMetric, localTag, onChange]);

  return (
    <div className={cn('border', 'rounded-1 mb-2')}>
      <div className="" style={{ margin: '-1px -1px -1px 0' }}>
        <div className="d-flex">
          <div className="flex-grow-1 d-flex py-1 px-2 align-content-center" onClick={onOpenToggle}>
            {!open && (
              <>
                {!localMetric ? (
                  <div className="font-monospace text-secondary text-truncate fw-bold">???</div>
                ) : (
                  <div className="font-monospace text-truncate fw-bold">{localMetric}</div>
                )}
                {!!localMetric && !!localTag && (
                  <div className="font-monospacer text-secondary text-truncate">
                    :&nbsp;{getTagDescription(meta, localTag)}
                  </div>
                )}
                <TagBadges
                  className="flex-grow-1 w-0 ms-2 overflow-hidden d-flex align-content-center"
                  filterIn={localFilterIn}
                  filterNotIn={localFilterNotIn}
                  // meta={meta}
                />
              </>
            )}
          </div>
          <div className="w-auto input-group input-group-sm">
            <ToggleButton className="btn btn-outline-primary rounded-start" checked={open} onChange={setOpen}>
              <SVGPencil />
            </ToggleButton>
            <Button className="btn btn-outline-danger" onClick={onRemove}>
              <SVGTrash />
            </Button>
          </div>
        </div>
      </div>
      {open && (
        <div className="px-2 mt-2">
          <div className="input-group input-group-sm mb-2">
            <SelectMetric value={localMetric} onChange={onChangeMetric} placeholder={placeholder} />
          </div>
          <div>
            {(meta?.tags || []).map((_t, indexTag) => {
              const tagKey = toTagKey(indexTag, TAG_KEY._0);
              return !tagKey || !isTagEnabled(meta, tagKey) ? null : (
                <div key={indexTag} className="form-check">
                  <input
                    className="form-check-input mt-2"
                    type="radio"
                    name={`input-${uid}`}
                    onChange={onRadioTag}
                    value={tagKey}
                    checked={tagKey === localTag}
                    // id={`input-${uid}-${tagKey}`}
                  />
                  <VariableControl<TagKey>
                    className="mb-2 form-check-label"
                    small
                    target={tagKey}
                    placeholder={getTagDescription(meta, indexTag)}
                    negative={negativeTags[tagKey]}
                    setNegative={onSetNegativeTag}
                    // groupBy={variableTags[tagKey]?.args.groupBy}
                    // setGroupBy={onSetGroupBy}
                    values={localFilterIn[tagKey]}
                    notValues={localFilterNotIn[tagKey]}
                    onChange={onFilterChange}
                    tagMeta={listTags[tagKey]?.tagMeta}
                    setOpen={onSetUpdateTag}
                    list={listTags[tagKey]?.list}
                    loaded={listTags[tagKey]?.loaded}
                    more={listTags[tagKey]?.more}
                    customValue={listTags[tagKey]?.more}
                  />
                </div>
              );
            })}
            {!isTagEnabled(meta, TAG_KEY._s) ? null : (
              <div className="form-check">
                <input
                  className="form-check-input mt-2"
                  type="radio"
                  name={`input-${uid}`}
                  onChange={onRadioTag}
                  value={TAG_KEY._s}
                  checked={TAG_KEY._s === localTag}
                  // id={`input-${uid}-${tagKey}`}
                />
                <VariableControl<TagKey>
                  className="mb-2 form-check-label"
                  small
                  target={TAG_KEY._s}
                  placeholder={getTagDescription(meta, -1)}
                  negative={negativeTags[TAG_KEY._s]}
                  setNegative={onSetNegativeTag}
                  // groupBy={variableTags[TAG_KEY._s]?.args.groupBy}
                  // setGroupBy={onSetGroupBy}
                  values={localFilterIn[TAG_KEY._s]}
                  notValues={localFilterNotIn[TAG_KEY._s]}
                  onChange={onFilterChange}
                  setOpen={onSetUpdateTag}
                  list={listTags[TAG_KEY._s]?.list}
                  loaded={listTags[TAG_KEY._s]?.loaded}
                  more={listTags[TAG_KEY._s]?.more}
                  customValue={listTags[TAG_KEY._s]?.more}
                  // customBadge={
                  //   variableTags[TAG_KEY._s] && (
                  //     <span
                  //       title={`is variable: ${variableTags[TAG_KEY._s]?.description || variableTags[TAG_KEY._s]?.name}`}
                  //       className={cn(
                  //         'input-group-text bg-transparent text-nowrap pt-0 pb-0 mt-2 me-2',
                  //         variableTags[TAG_KEY._s]?.args.negative ?? negativeTags[TAG_KEY._s]
                  //           ? 'border-danger text-danger'
                  //           : 'border-success text-success'
                  //       )}
                  //     >
                  //       <span className="small">{variableTags[TAG_KEY._s]?.name}</span>
                  //     </span>
                  //   )
                  // }
                />
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
