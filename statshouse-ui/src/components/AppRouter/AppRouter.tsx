// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useLayoutEffect } from 'react';
import { Router } from 'react-router-dom';
import { appHistory } from '@/common/appHistory';

export type AppRouterProps = { children?: React.ReactNode };
export function AppRouter({ children }: AppRouterProps) {
  const [state, setState] = React.useState({
    action: appHistory.action,
    location: appHistory.location,
  });
  useLayoutEffect(() => appHistory.listen(setState), []);
  return (
    <Router location={state.location} navigationType={state.action} navigator={appHistory}>
      {children}
    </Router>
  );
}
