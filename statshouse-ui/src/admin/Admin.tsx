// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { FormPage } from './pages/FormPage';
import { CreatePage } from './pages/CreatePage';
import { Outlet, Route, Routes } from 'react-router-dom';
import { AdminDashControl } from './AdminDashControl';
import { MetricHistory } from '@/admin/pages/MetricHistory';
import { MetricEditMenu } from '@/components2/MetricEdit/MetricEditMenu';

export function Admin(props: { yAxisSize: number; adminMode: boolean }) {
  const { yAxisSize, adminMode } = props;

  return (
    <Routes>
      <Route path="create" element={<CreatePage yAxisSize={yAxisSize} />} />
      <Route
        path="edit/:metricName"
        element={
          <div className="container-xl pt-3 pb-3">
            <MetricEditMenu />
            <Outlet />
          </div>
        }
      >
        <Route path="" element={<FormPage adminMode={adminMode} yAxisSize={yAxisSize} />} />
        <Route path="history" element={<MetricHistory adminMode={adminMode} />} />
      </Route>
      <Route path="dash/" element={<AdminDashControl />} />
    </Routes>
  );
}

export default Admin;
