// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { Suspense, useEffect } from 'react';
import { Admin } from './admin/Admin';
import { Navigate, Outlet, Route, Routes, useLocation } from 'react-router-dom';
import { ViewPage } from './view/ViewPage';
import { BuildVersion, HeaderMenu } from './components';
import { currentAccessInfo } from './common/access';
import { DashboardListView } from './view/DashboardListView';

const FAQ = React.lazy(() => import('./doc/FAQ'));

const yAxisSize = 54; // must be synced with .u-legend padding-left

function App() {
  const ai = currentAccessInfo();
  return (
    <Routes>
      <Route path="/" element={<Navigate to="view" replace={true} />} />
      <Route path="embed" element={<ViewPage embed={true} yAxisSize={yAxisSize} />} />
      <Route path="/" element={<NavbarApp />}>
        <Route
          path="doc/faq"
          element={
            <Suspense fallback={<div>FAQ Loading...</div>}>
              <FAQ yAxisSize={yAxisSize} />
            </Suspense>
          }
        />
        <Route path="admin/*" element={<Admin yAxisSize={yAxisSize} adminMode={ai.admin} />} />
        {/*<Route path="settings/*" element={<SettingsPage adminMode={ai.admin} />}>*/}
        {/*<Route path="group" element={<GroupPage />} />*/}
        {/*<Route path="prometheus" element={<PrometheusPage />} />*/}
        {/*</Route>*/}

        <Route path="view" element={<ViewPage />} />
        <Route path="dash-list" element={<DashboardListView />} />
        <Route path="*" element={<NotFound />} />
      </Route>
    </Routes>
  );
}

const NavbarApp = function _NavbarApp() {
  const globalWarning: string = '';
  return (
    <div className="d-flex flex-row min-vh-100 position-relative">
      <HeaderMenu />
      <div className="flex-grow-1 w-0 d-flex flex-column">
        {globalWarning !== '' && <div className="alert-warning rounded px-2 py-1">{globalWarning}</div>}
        <Outlet />
        <BuildVersion className="text-end text-secondary build-version container-xl pb-3" />
      </div>
    </div>
  );
};

function NotFound() {
  const location = useLocation();

  useEffect(() => {
    document.title = `404 — StatsHouse`;
  }, []);

  return (
    <div className="container-xl pt-3 pb-3">
      <p className="text-center pt-5">
        <code>{location.pathname}</code> — page not found.
      </p>
    </div>
  );
}

export default App;
