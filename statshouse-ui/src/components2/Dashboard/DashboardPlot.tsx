// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React from 'react';

import css from './style.module.css';

export type DashboardPlotProps = {
  children?: React.ReactNode;
};

export function DashboardPlot({ children }: DashboardPlotProps) {
  return (
    <div className={css.dashboardPlot}>
      <div>{children}</div>
    </div>
  );
}
