// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo, useCallback, useContext, useEffect, useRef, useState } from 'react';
import { ReactComponent as SVGXSquare } from 'bootstrap-icons/icons/x-square.svg';
import { ReactComponent as SVGFlagFill } from 'bootstrap-icons/icons/flag-fill.svg';
import { ReactComponent as SVGTrash } from 'bootstrap-icons/icons/trash.svg';

import { useIntersectionObserver, useOnClickOutside, useStateBoolean, useStateToRef } from '@/hooks';
import cn from 'classnames';
import css from './style.module.css';
import { Link } from 'react-router-dom';
import { Button, Popper } from '@/components/UI';
import { PLOT_TYPE } from '@/api/enum';
import { PlotName } from '../Plot/PlotView/PlotName';
import { usePlotLoader } from '@/store2/plotQueryStore';
import { PlotLink } from '../Plot/PlotLink';
import { setPlotPreviewVisibility } from '@/store2/plotVisibilityStore';
import { usePlotPreviewStore } from '@/store2/plotPreviewStore';
import { useLinkPlot } from '@/hooks/useLinkPlot';
import { useMetricName } from '@/hooks/useMetricName';
import { useStatsHouse } from '@/store2';
import { usePlotsDataStore } from '@/store2/plotDataStore';
import { selectorOrderPlot } from '@/store2/selectors';
import { removePlot } from '@/store2/methods';
import { WidgetPlotContext } from '@/contexts/WidgetPlotContext';

const stopPropagation = (e: React.MouseEvent) => {
  e.stopPropagation();
};

export type LeftMenuPlotItemProps = {
  active?: boolean;
};

export const LeftMenuPlotItem = memo(function LeftMenuPlotItem({ active }: LeftMenuPlotItemProps) {
  const itemRef = useRef(null);
  const [visibleRef, setVisibleRef] = useState<HTMLElement | null>(null);
  const sub = useRef<HTMLUListElement>(null);
  const [open, setOpen] = useStateBoolean(false);
  const openRef = useStateToRef(open);
  useOnClickOutside(itemRef, setOpen.off);
  const plotKey = useContext(WidgetPlotContext);

  const onClick = useCallback(
    (event: React.MouseEvent) => {
      if (!openRef.current) {
        setOpen.on();
        event.preventDefault();
        event.stopPropagation();
      }
    },
    [openRef, setOpen]
  );
  const visible = useIntersectionObserver(visibleRef, 0, undefined, 0);
  const visibleBool = visible > 0;
  const plotPreviewUrl = usePlotPreviewStore(useCallback((s) => s.plotPreviewUrlList[plotKey], [plotKey]));

  const plotType = useStatsHouse(useCallback(({ params: { plots } }) => plots[plotKey]?.type, [plotKey]));

  const orderPlot = useStatsHouse(selectorOrderPlot);
  const canRemove = orderPlot.length > 1;
  const metricName = useMetricName();

  const plotLoader = usePlotLoader(plotKey);

  const error = usePlotsDataStore(useCallback(({ plotsData }) => plotsData[plotKey]?.error, [plotKey]));
  const error403 = usePlotsDataStore(useCallback(({ plotsData }) => plotsData[plotKey]?.error403, [plotKey]));

  const onRemove = useCallback(
    (e: React.MouseEvent) => {
      removePlot(plotKey);
      e.stopPropagation();
      e.preventDefault();
    },
    [plotKey]
  );

  const link = useLinkPlot(plotKey, visibleBool);

  useEffect(() => {
    setPlotPreviewVisibility(plotKey, visibleBool);
  }, [plotKey, visibleBool]);

  return (
    <li
      className={cn(css.leftMenuItem, active && css.active)}
      ref={itemRef}
      onMouseOver={setOpen.on}
      onMouseOut={setOpen.off}
      onClick={setOpen.off}
    >
      <Link
        ref={setVisibleRef}
        className={cn(
          css.link,
          !error403 ? css.preview : css.preview403,
          plotType === PLOT_TYPE.Event && css.previewEvent
        )}
        to={link}
        onClick={onClick}
      >
        {!!error403 && <SVGXSquare className={css.icon} />}
        {!!plotPreviewUrl && !error403 && <img alt={metricName} src={plotPreviewUrl} className="w-100 h-100" />}
        {plotLoader && !error403 && !error && (
          <div className="position-absolute top-50 start-50 translate-middle show-delay">
            <div className="spinner-white-bg spinner-border spinner-border-sm" role="status" aria-hidden="true"></div>
          </div>
        )}
        {plotType === PLOT_TYPE.Event && <SVGFlagFill className="position-absolute top-0 start-0 ms-1 mt-1" />}
      </Link>
      <Popper targetRef={itemRef} fixed={false} horizontal={'out-right'} vertical={'top'} show={open} always>
        <ul className={css.sub} ref={sub}>
          <li className={cn(css.subItem, 'font-monospace fw-bold text-center p-1')}>
            <PlotLink className={cn('d-flex overflow-hidden align-items-center p-0', css.link)} plotKey={plotKey}>
              <PlotName className="flex-grow-1 d-flex overflow-hidden" />
              {canRemove && (
                <Button
                  className={cn('btn btn-sm ms-1 border-0')}
                  title="Remove"
                  onPointerDown={stopPropagation}
                  onClick={onRemove}
                  type="button"
                >
                  <SVGTrash />
                </Button>
              )}
            </PlotLink>
          </li>
          {!!plotPreviewUrl && !error403 && (
            <li className="nav-item p-1">
              <img alt={metricName} src={plotPreviewUrl} className={css.bigPreview} />
            </li>
          )}
        </ul>
      </Popper>
    </li>
  );
});
