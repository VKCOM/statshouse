// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';
import { useCallback, useMemo, useState } from 'react';
import { useMetricName } from '@/hooks/useMetricName';
import { useMetricMeta } from '@/hooks/useMetricMeta';
import cn from 'classnames';
import { Tooltip } from '@/components/UI';
import css from './style.module.css';
import { ReactComponent as SVGBoxArrowUpRight } from 'bootstrap-icons/icons/box-arrow-up-right.svg';
import { ReactComponent as SVGChevronUp } from 'bootstrap-icons/icons/chevron-up.svg';
import { ReactComponent as SVGChevronDown } from 'bootstrap-icons/icons/chevron-down.svg';
import { PlotLink } from '@/components2/Plot/PlotLink';
import { PlotName } from '@/components2/Plot/PlotView/PlotName';
import { PlotHeaderBadgeResolution } from '@/components2/Plot/PlotView/PlotHeaderBadgeResolution';
import { PlotHeaderBadges } from '@/components2/Plot/PlotView/PlotHeaderBadges';
import { PlotHeaderTooltipContent } from '@/components2/Plot/PlotView/PlotHeaderTooltipContent';

export function PlotHeaderEmbedDashboard() {
  const { plot } = useWidgetPlotContext();

  const [showTags, setShowTags] = useState(false);

  const meta = useMetricMeta(useMetricName(true));

  const description = plot?.customDescription || meta?.description;

  const toggleShowTags = useCallback(() => {
    setShowTags((s) => !s);
  }, []);

  const plotTooltip = useMemo(() => {
    const desc = description || '';
    return <PlotHeaderTooltipContent name={<PlotName />} description={desc} />;
  }, [description]);

  return (
    <div className="font-monospace fw-bold">
      <div className={cn('d-flex position-relative w-100', !plot?.customName && !showTags && 'pe-4')}>
        <div className="flex-grow-1 w-50 px-1 d-flex">
          <Tooltip hover as="span" className="text-decoration-none overflow-hidden text-nowrap" title={plotTooltip}>
            <PlotLink plotKey={plot.id} className="text-decoration-none" target={'_blank'}>
              <PlotName />
            </PlotLink>
          </Tooltip>

          <PlotLink plotKey={plot.id} className="ms-2" single target="_blank">
            <SVGBoxArrowUpRight width={10} height={10} />
          </PlotLink>
        </div>
        {!plot?.customName ? (
          <>
            <div
              className={cn(
                css.badge,
                'd-flex gap-1 z-2 flex-row',
                showTags
                  ? 'position-absolute bg-body end-0 top-0 flex-wrap align-items-end justify-content-end pt-4 p-1'
                  : 'overflow-hidden  flex-nowrap',
                showTags ? css.badgeShow : css.badgeHide
              )}
            >
              <PlotHeaderBadges compact dashboard className={cn(showTags ? 'text-wrap' : 'text-nowrap')} />
            </div>
            <div role="button" onClick={toggleShowTags} className="z-2 px-1 position-absolute end-0 top-0">
              {showTags ? <SVGChevronUp width="12px" height="12px" /> : <SVGChevronDown width="12px" height="12px" />}
            </div>
          </>
        ) : (
          <PlotHeaderBadgeResolution resolution={meta?.resolution} customAgg={plot?.customAgg} />
        )}
      </div>
    </div>
  );
}
