// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { Suspense } from 'react';
import { createRoot } from 'react-dom/client';
import './index.scss';
import reportWebVitals from './reportWebVitals';
import { AppRouter } from 'components/AppRouter';
import './api/stat'; // global error log

const appVersion = localStorage.getItem('appVersion');

const App = React.lazy(() => {
  if (appVersion === '1') {
    return import('./AppOld');
  } else {
    return import('./App');
  }
});

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

reportWebVitals(undefined);
