// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React from 'react';
import css from './style.module.css';
import cn from 'classnames';
export type PlotPanelProps = {
  children?: React.ReactNode;
  className?: string;
};
export function PlotPanel({ children, className }: PlotPanelProps) {
  return (
    <div className={css.plotPanelWrap}>
      {/*<div className={css.plotPanelLayout}>*/}
      <div className={cn(css.plotPanel, className)}>{children}</div>
      {/*</div>*/}
    </div>
  );
}
