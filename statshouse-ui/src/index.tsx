// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { Suspense } from 'react';
import { createRoot } from 'react-dom/client';
import './index.scss';
import { App } from './App';
import { AppRouter } from '@/components/AppRouter';
import './api/stat';

const root = createRoot(document.getElementById('root')!);
root.render(
  <React.StrictMode>
    <AppRouter>
      <Suspense fallback={<div>Loading...</div>}>
        <App />
      </Suspense>
    </AppRouter>
  </React.StrictMode>
);
