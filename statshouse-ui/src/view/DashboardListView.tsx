// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useEffect, useMemo } from 'react';
import { useStateInput } from '@/hooks';
import { ErrorMessages } from '@/components/ErrorMessages';
import cn from 'classnames';
import { emptyArray, SearchFabric } from '@/common/helpers';
import { useWindowSize } from '@/hooks/useWindowSize';
import { useFavoriteStore } from '@/store2/favoriteStore';
import { selectApiDashboardList, useApiDashboardList } from '@/api/dashboardsList';
import { useDebounceValue } from '@/hooks/useDebounceValue';
import { DashboardListItem } from '@/components2';
import { pageTitle } from '@/store2';

export type DashboardListViewProps = {};

export const DashboardListView: React.FC<DashboardListViewProps> = () => {
  const query = useApiDashboardList(selectApiDashboardList);
  const list = query.data ?? emptyArray;
  const dashboardsFavorite = useFavoriteStore((s) => s.dashboardsFavorite);
  const scrollY = useWindowSize((s) => s.scrollY > 16);
  const searchInput = useStateInput('');
  const searchDebounce = useDebounceValue(searchInput.value);

  const filterListFavorite = useMemo(() => {
    const res = list
      .filter((v) => dashboardsFavorite[v.id])
      .filter(SearchFabric(searchDebounce, ['name', 'description']));
    res.sort((a, b) =>
      a.name.toLowerCase() > b.name.toLowerCase() ? 1 : a.name.toLowerCase() < b.name.toLowerCase() ? -1 : 0
    );
    return res;
  }, [dashboardsFavorite, list, searchDebounce]);

  const filterList = useMemo(() => {
    const res = list
      .filter((v) => !dashboardsFavorite[v.id])
      .filter(SearchFabric(searchDebounce, ['name', 'description']));
    res.sort((a, b) =>
      a.name.toLowerCase() > b.name.toLowerCase() ? 1 : a.name.toLowerCase() < b.name.toLowerCase() ? -1 : 0
    );
    return res;
  }, [dashboardsFavorite, list, searchDebounce]);

  useEffect(() => {
    document.title = `Dashboard list — ${pageTitle}`;
  }, []);

  return (
    <div className="container-sm pt-3 pb-3 w-max-720">
      <div className={cn('mb-2 ', scrollY && 'sticky-top')}>
        <input
          id="dashboard-list-search"
          type="search"
          placeholder="Search"
          className={cn('form-control', scrollY && 'shadow')}
          aria-label="search"
          {...searchInput}
        />
      </div>
      <ErrorMessages />
      <ul className="list-group mb-2">
        {filterListFavorite.map((item) => (
          <DashboardListItem key={item.id} item={item} />
        ))}
      </ul>
      <ul className="list-group">
        {filterList.map((item) => (
          <DashboardListItem key={item.id} item={item} />
        ))}
      </ul>
    </div>
  );
};

export default DashboardListView;
