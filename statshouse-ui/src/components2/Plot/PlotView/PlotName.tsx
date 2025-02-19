// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { memo, useMemo } from 'react';
import cn from 'classnames';
import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';
import { useMetricName } from '@/hooks/useMetricName';
import { useMetricWhats } from '@/hooks/useMetricWhats';

export type PlotNameProps = {
  className?: string;
};
export const PlotName = memo(function PlotName({ className }: PlotNameProps) {
  const {
    plot: { customName },
  } = useWidgetPlotContext();
  const metricName = useMetricName();
  const whats = useMetricWhats();
  const what = useMemo(() => whats.join(', '), [whats]);

  if (customName) {
    return <span className={cn(className, 'text-body text-truncate')}>{customName}</span>;
  }
  if (metricName) {
    return (
      <span className={cn(className)}>
        <span className="text-body text-truncate">{metricName}</span>
        {!!what && <span className="text-secondary text-truncate">:&nbsp;{what}</span>}
      </span>
    );
  }
  return <span className={cn(className)}>&nbsp;</span>;
});
