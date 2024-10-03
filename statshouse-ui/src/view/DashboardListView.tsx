// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { DashboardListStore, useDashboardListStore } from '../store/dashboardList';
import React, { useEffect, useMemo } from 'react';
import { Link } from 'react-router-dom';
import { useStateInput } from '../hooks';
import { ErrorMessages } from '../components/ErrorMessages';
import cn from 'classnames';
import { SearchFabric } from '../common/helpers';
import { useWindowSize } from 'hooks/useWindowSize';
import { toggleDashboardsFavorite, useFavoriteStore } from 'store2/favoriteStore';
import { ReactComponent as SVGStar } from 'bootstrap-icons/icons/star.svg';
import { ReactComponent as SVGStarFill } from 'bootstrap-icons/icons/star-fill.svg';
import { Tooltip } from '../components/UI';

export type DashboardListViewProps = {};

const { update } = useDashboardListStore.getState();
const selectorDashboardList = ({ list }: DashboardListStore) => list;

export const DashboardListView: React.FC<DashboardListViewProps> = () => {
  const list = useDashboardListStore(selectorDashboardList);
  const dashboardsFavorite = useFavoriteStore((s) => s.dashboardsFavorite);
  const scrollY = useWindowSize((s) => s.scrollY > 16);
  const searchInput = useStateInput('');
  useEffect(() => {
    update();
  }, []);

  const filterListFavorite = useMemo(() => {
    const res = list
      .filter((v) => dashboardsFavorite[v.id])
      .filter(SearchFabric(searchInput.value, ['name', 'description']));
    res.sort((a, b) =>
      a.name.toLowerCase() > b.name.toLowerCase() ? 1 : a.name.toLowerCase() < b.name.toLowerCase() ? -1 : 0
    );
    return res;
  }, [dashboardsFavorite, list, searchInput.value]);

  const filterList = useMemo(() => {
    const res = list
      .filter((v) => !dashboardsFavorite[v.id])
      .filter(SearchFabric(searchInput.value, ['name', 'description']));
    res.sort((a, b) =>
      a.name.toLowerCase() > b.name.toLowerCase() ? 1 : a.name.toLowerCase() < b.name.toLowerCase() ? -1 : 0
    );
    return res;
  }, [dashboardsFavorite, list, searchInput.value]);

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
          <li key={item.id} className="list-group-item">
            <Link to={`/view?id=${item.id}`} className="text-body text-decoration-none">
              <h6 className="m-0 d-flex align-items-center gap-1">
                <span className="flex-grow-1">{item.name}</span>
                <Tooltip title={dashboardsFavorite[item.id] ? 'remove favorite' : 'add favorite'}>
                  <span
                    className="text-primary"
                    onClick={(e) => {
                      toggleDashboardsFavorite(item.id);
                      e.stopPropagation();
                      e.preventDefault();
                    }}
                  >
                    {dashboardsFavorite[item.id] ? <SVGStarFill /> : <SVGStar />}
                  </span>
                </Tooltip>
              </h6>
              {!!item.description && <div className="small text-secondary mt-2">{item.description}</div>}
            </Link>
          </li>
        ))}
      </ul>
      <ul className="list-group">
        {filterList.map((item) => (
          <li key={item.id} className="list-group-item">
            <Link to={`/view?id=${item.id}`} className="text-body text-decoration-none">
              <h6 className="m-0 d-flex gap-1">
                <span className="flex-grow-1">{item.name}</span>
                <Tooltip title={dashboardsFavorite[item.id] ? 'remove favorite' : 'add favorite'}>
                  <span
                    className="text-primary"
                    onClick={(e) => {
                      toggleDashboardsFavorite(item.id);
                      e.stopPropagation();
                      e.preventDefault();
                    }}
                  >
                    {dashboardsFavorite[item.id] ? <SVGStarFill /> : <SVGStar />}
                  </span>
                </Tooltip>
              </h6>
              {!!item.description && <div className="small text-secondary mt-2">{item.description}</div>}
            </Link>
          </li>
        ))}
      </ul>
    </div>
  );
};

export default DashboardListView;
