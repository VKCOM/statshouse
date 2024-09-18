// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import * as React from 'react';
import { FormPage } from './pages/FormPage';
import { CreatePage } from './pages/CreatePage';
import { Route, Routes } from 'react-router-dom';

export function Admin(props: { yAxisSize: number; adminMode: boolean }) {
  const { yAxisSize, adminMode } = props;

  return (
    <Routes>
      <Route path="create" element={<CreatePage yAxisSize={yAxisSize} />} />
      <Route path="edit/:metricName" element={<FormPage adminMode={adminMode} yAxisSize={yAxisSize} />} />
    </Routes>
  );
}

export default Admin;
