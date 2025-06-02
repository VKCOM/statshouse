// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';
import { useMemo } from 'react';
import { useMetricName } from '@/hooks/useMetricName';
import { useMetricMeta } from '@/hooks/useMetricMeta';
import cn from 'classnames';
import { Tooltip } from '@/components/UI';
import { PlotLink } from '@/components2/Plot/PlotLink';
import { PlotName } from '@/components2/Plot/PlotView/PlotName';
import { PlotHeaderBadges } from '@/components2/Plot/PlotView/PlotHeaderBadges';
import { PlotHeaderTooltipContent } from '@/components2/Plot/PlotView/PlotHeaderTooltipContent';

export function PlotHeaderEmbed() {
  const { plot } = useWidgetPlotContext();
  const meta = useMetricMeta(useMetricName(true));
  const description = plot?.customDescription || meta?.description;

  const plotTooltip = useMemo(() => {
    const desc = description || '';
    return <PlotHeaderTooltipContent name={<PlotName />} description={desc} />;
  }, [description]);

  return (
    <div className={cn('d-flex flex-grow-1 flex-wrap', 'justify-content-around')}>
      <h6
        className={`d-flex flex-wrap justify-content-center align-items-center overflow-force-wrap font-monospace fw-bold me-3 flex-grow-1 gap-1 mb-1`}
      >
        <Tooltip hover title={plotTooltip}>
          <PlotLink plotKey={plot.id} className="text-secondary text-decoration-none" target="_blank">
            <PlotName />
          </PlotLink>
        </Tooltip>
        <PlotHeaderBadges compact />
      </h6>
    </div>
  );
}
