// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { selectorListServerDashboard, selectorLoadListServerDashboard, useStore } from '../store';
import React, { useEffect, useMemo } from 'react';
import { Link } from 'react-router-dom';
import { useStateInput } from '../hooks';
import { sortByKey } from './utils';

export type DashboardListViewProps = {};

export const DashboardListView: React.FC<DashboardListViewProps> = () => {
  const listServerDashboard = useStore(selectorListServerDashboard);
  const loadListServerDashboard = useStore(selectorLoadListServerDashboard);
  const searchInput = useStateInput('');
  useEffect(() => {
    loadListServerDashboard();
  }, [loadListServerDashboard]);

  const filterList = useMemo(() => {
    const res = listServerDashboard.filter(
      (item) =>
        searchInput.value === '' ||
        item.name.includes(searchInput.value) ||
        item.description.includes(searchInput.value)
    );
    res.sort(sortByKey.bind(null, 'name'));
    return res;
  }, [listServerDashboard, searchInput.value]);

  return (
    <div className="container-sm pt-3 pb-3 w-max-720">
      <div className="mb-2 row">
        <label htmlFor="dashboard-list-search" className="col-sm-2 col-form-label">
          Search
        </label>
        <div className="col-sm-10">
          <input
            id="dashboard-list-search"
            type="search"
            className="form-control"
            aria-label="search"
            {...searchInput}
          />
        </div>
      </div>
      <ul className="list-group">
        {filterList.map((item) => (
          <li key={item.id} className="list-group-item">
            <Link to={`/view?id=${item.id}`} className="text-black text-decoration-none">
              <h6>{item.name}</h6>
              {!!item.description && <div className="small text-secondary">{item.description}</div>}
            </Link>
          </li>
        ))}
      </ul>
    </div>
  );
};
