// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React from 'react';
import { Link, To } from 'react-router-dom';
import { useLinkListStore } from '@/store/linkList/linkListStore';

export type PlotLinkProps = {
  indexPlot?: number;
  isLink?: boolean;
  to?: To;
  newPlot?: boolean;
} & Omit<React.AnchorHTMLAttributes<HTMLAnchorElement>, 'href'> &
  React.RefAttributes<HTMLAnchorElement>;

export const PlotLink: React.ForwardRefExoticComponent<PlotLinkProps> = React.forwardRef<
  HTMLAnchorElement,
  PlotLinkProps
>(function PlotLink({ indexPlot, isLink, to, children, newPlot, ...attributes }, ref) {
  const plotSearch = useLinkListStore(({ links }) => (indexPlot != null && links[indexPlot.toString()]) || '');

  return (
    <Link to={to ?? { pathname: '/view', search: plotSearch }} {...attributes} ref={ref}>
      {children}
    </Link>
  );
});
