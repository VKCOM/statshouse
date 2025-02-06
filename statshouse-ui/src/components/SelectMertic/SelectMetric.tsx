// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { memo, useCallback, useEffect, useMemo, useState } from 'react';
import { useMetricsListStore } from '@/store2/metricsList';
import cn from 'classnames';
import { Select, SelectOptionProps } from '../UI/Select';
import { useDebounceState } from '@/hooks';
import { SearchFabric } from '@/common/helpers';
import { toggleShowMetricsFavorite, useFavoriteStore } from '@/store2/favoriteStore';
import { SelectMetricRow } from './SelectMetricRow';
import { ToggleShowMetricsFavorite } from './ToggleShowMetricsFavorite';

export type SelectMetricProps = {
  value?: string;
  onChange?: (value?: string | string[]) => void;
  className?: string;
  placeholder?: string;
};

export const SelectMetric = memo(function SelectMetric({ value, onChange, className, placeholder }: SelectMetricProps) {
  const { list, loading } = useMetricsListStore();
  const metricsFavorite = useFavoriteStore((s) => s.metricsFavorite);
  const showMetricsFavorite = useFavoriteStore((s) => s.showMetricsFavorite);

  const [search, searchDebounce, setSearch] = useDebounceState(value);
  const [noSearch, setNoSearch] = useState(true);
  const favoriteList = useMemo(
    () =>
      list
        .filter((v) => metricsFavorite[v.name])
        .map(({ name }) => ({
          value: name,
          checked: name === value,
        })),
    [list, metricsFavorite, value]
  );

  const filterOptions = useMemo<SelectOptionProps[]>(() => {
    let l = [...list];
    if (searchDebounce && !noSearch) {
      l = l.filter(SearchFabric(searchDebounce, ['name']));
    }
    return l.map(({ name }) => ({
      value: name,
      checked: name === value,
    }));
  }, [list, noSearch, searchDebounce, value]);

  const onOpen = useCallback(() => {
    setSearch(value ?? '');
    setNoSearch(true);
  }, [setSearch, value]);

  const onSearch = useCallback(
    (v: string) => {
      setNoSearch(false);
      setSearch(v);
    },
    [setSearch]
  );
  const onChangeValue = useCallback(
    (values: SelectOptionProps[]) => {
      onChange?.(values[0]?.value);
    },
    [onChange]
  );
  useEffect(() => {
    if (!favoriteList.length && !loading) {
      toggleShowMetricsFavorite(false);
    }
  }, [favoriteList.length, loading, showMetricsFavorite]);

  return (
    <>
      <Select<SelectOptionProps>
        className={cn(className)}
        options={showMetricsFavorite && noSearch && favoriteList.length ? favoriteList : filterOptions}
        search={search}
        placeholder={placeholder ?? value}
        onSearch={onSearch}
        onOpen={onOpen}
        onClose={onOpen}
        onChange={onChangeValue}
        minWidth={300}
        itemSize={30}
        loading={loading}
        selectButtons={
          favoriteList.length || showMetricsFavorite ? (
            <ToggleShowMetricsFavorite status={noSearch && showMetricsFavorite && (!!favoriteList.length || loading)} />
          ) : undefined
        }
      >
        {SelectMetricRow}
      </Select>
    </>
  );
});
