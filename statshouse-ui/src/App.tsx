// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import '@/store2';
import React, { Suspense, useEffect } from 'react';
import { Navigate, Route, Routes, useLocation } from 'react-router-dom';
import { QueryClientProvider } from '@tanstack/react-query';
import { queryClient } from './common/queryClient';
import { useStatsHouse } from '@/store2';
import View2Page from './view2/ViewPage';
import Core from './view2/Core';
import { ReactQueryDevtools } from '@tanstack/react-query-devtools';

const FAQ = React.lazy(() => import('./doc/FAQ'));
const Admin = React.lazy(() => import('./admin/Admin'));
const SettingsPage = React.lazy(() => import('./view/Settings/SettingsPage'));
const GroupPage = React.lazy(() => import('./view/Settings/GroupPage'));
const NamespacePage = React.lazy(() => import('./view/Settings/NamespacePage'));
const DashboardListView = React.lazy(() => import('./view/DashboardListView'));

const yAxisSize = 54; // must be synced with .u-legend padding-left

export function App() {
  const isAdmin = useStatsHouse((s) => s.user.admin);
  return (
    <QueryClientProvider client={queryClient}>
      {process.env.NODE_ENV !== 'production' && <ReactQueryDevtools client={queryClient} />}
      <Routes>
        <Route path="/" element={<Core />}>
          <Route path="" element={<Navigate to="view" replace={true} />} />
          <Route path="view" element={<View2Page />} />
          <Route path="embed" element={<View2Page />} />
          <Route path="settings/*" element={<SettingsPage adminMode={isAdmin} />}>
            <Route path="group" element={<GroupPage />} />
            <Route path="namespace" element={<NamespacePage />} />
          </Route>
          <Route
            path="dash-list"
            element={
              <Suspense fallback={<div>Loading...</div>}>
                <DashboardListView />
              </Suspense>
            }
          />
          <Route
            path="doc/faq"
            element={
              <Suspense fallback={<div>FAQ Loading...</div>}>
                <FAQ yAxisSize={yAxisSize} />
              </Suspense>
            }
          />
          <Route
            path="admin/*"
            element={
              <Suspense fallback={<div>Loading...</div>}>
                <Admin yAxisSize={yAxisSize} adminMode={isAdmin} />
              </Suspense>
            }
          />
          <Route path="*" element={<NotFound />} />
        </Route>
      </Routes>
    </QueryClientProvider>
  );
}

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
