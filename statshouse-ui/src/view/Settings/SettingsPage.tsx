// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React from 'react';
import { NavLink, Outlet } from 'react-router-dom';

export type SettingsPageProps = {
  adminMode: boolean;
};
export const SettingsPage: React.FC<SettingsPageProps> = ({ adminMode }) => {
  if (!adminMode) {
    return (
      <div className="text-bg-light w-100 h-100 position-absolute top-0 start-0 d-flex align-items-center justify-content-center">
        Access denied
      </div>
    );
  }
  return (
    <div className="d-flex flex-column flex-grow-1">
      <ul className="nav nav-tabs px-2 pt-1">
        <li className="nav-item">
          <NavLink className="nav-link" to="/settings/group" end aria-current="page">
            Group
          </NavLink>
        </li>
        <li className="nav-item">
          <NavLink className="nav-link" to="/settings/namespace" end aria-current="page">
            Namespace
          </NavLink>
        </li>
        {/*<li className="nav-item">
          <NavLink className="nav-link" to="./prometheus" end aria-current="page">
            Prometheus
          </NavLink>
        </li>*/}
      </ul>
      <div className="flex-grow-1 d-flex flex-column">
        <Outlet />
      </div>
    </div>
  );
};

export default SettingsPage;
