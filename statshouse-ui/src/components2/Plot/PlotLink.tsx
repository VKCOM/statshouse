import React from 'react';
import { PlotKey } from 'url2';
import { Link } from 'react-router-dom';
import { useLinkPlot } from '../../hooks';

export type PlotLinkProps = {
  plotKey: PlotKey;
  className?: string;
  children?: React.ReactNode;
  target?: React.HTMLAttributeAnchorTarget;
  single?: boolean;
};

export function PlotLink({ children, plotKey, className, target, single }: PlotLinkProps) {
  const link = useLinkPlot(plotKey, undefined, single);
  return (
    <Link className={className} to={link} target={target}>
      {children}
    </Link>
  );
}
