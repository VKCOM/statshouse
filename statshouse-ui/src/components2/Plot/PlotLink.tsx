// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo } from 'react';
import { PlotKey } from '@/url2';
import { Link } from 'react-router-dom';
import { useLinkPlot } from '@/hooks/useLinkPlot';

export type PlotLinkProps = {
  plotKey: PlotKey;
  className?: string;
  children?: React.ReactNode;
  target?: React.HTMLAttributeAnchorTarget;
  single?: boolean;
};

export const PlotLink = memo(function PlotLink({ children, plotKey, className, target, single }: PlotLinkProps) {
  const link = useLinkPlot(plotKey, undefined, single);
  return (
    <Link className={className} to={link} target={target}>
      {children}
    </Link>
  );
});
