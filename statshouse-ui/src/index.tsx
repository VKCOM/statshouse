// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React from 'react';
import { createRoot } from 'react-dom/client';
import * as ReactRouterDOM from 'react-router-dom';
import './index.scss';
import App from './App';
import reportWebVitals from './reportWebVitals';
const root = createRoot(document.getElementById('root')!);
root.render(
  <React.StrictMode>
    <ReactRouterDOM.BrowserRouter>
      <App />
    </ReactRouterDOM.BrowserRouter>
  </React.StrictMode>
);

reportWebVitals(undefined);
