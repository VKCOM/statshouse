import React from 'react';
import { type PlotControlProps } from './PlotControl';
import { ErrorMessages } from 'components';

export function PlotControlPromQL({ className }: PlotControlProps) {
  return (
    <div className={className}>
      <ErrorMessages />
    </div>
  );
}
