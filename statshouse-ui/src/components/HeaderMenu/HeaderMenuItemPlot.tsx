// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useLocation } from 'react-router-dom';

import { ReactComponent as SVGTrash } from 'bootstrap-icons/icons/trash.svg';
import { ReactComponent as SVGXSquare } from 'bootstrap-icons/icons/x-square.svg';

import {
  selectorNumQueriesPlotByIndex,
  selectorParamsPlotsByIndex,
  selectorParamsTabNum,
  selectorPlotsDataByIndex,
  selectorPreviewsByIndex,
  selectorRemovePlot,
  useStore,
} from '../../store';
import { PlotLink } from '../Plot/PlotLink';
import { whatToWhatDesc } from '../../view/api';

import cn from 'classnames';
import css from './style.module.css';
import { promQLMetric } from '../../view/utils';

export type HeaderMenuItemPlotProps = {
  indexPlot: number;
};

export const HeaderMenuItemPlot: React.FC<HeaderMenuItemPlotProps> = ({ indexPlot }) => {
  const touchToggle = useRef<HTMLAnchorElement>(null);
  const sub = useRef<HTMLUListElement>(null);
  const [open, setOpen] = useState(false);
  const location = useLocation();
  const isView = location.pathname === '/view';
  const selectorParamsPlot = useMemo(() => selectorParamsPlotsByIndex.bind(undefined, indexPlot), [indexPlot]);
  const plot = useStore(selectorParamsPlot);
  const selectorPreview = useMemo(() => selectorPreviewsByIndex.bind(undefined, indexPlot), [indexPlot]);
  const preview = useStore(selectorPreview);
  const removePlot = useStore(selectorRemovePlot);
  const selectorNumQueries = useMemo(() => selectorNumQueriesPlotByIndex.bind(undefined, indexPlot), [indexPlot]);
  const numQueries = useStore(selectorNumQueries);
  const selectorData = useMemo(() => selectorPlotsDataByIndex.bind(undefined, indexPlot), [indexPlot]);
  const data = useStore(selectorData);

  const onRemovePlot = useCallback(() => {
    removePlot(indexPlot);
  }, [indexPlot, removePlot]);

  const tabNum = useStore(selectorParamsTabNum);

  const active = useRef(false);

  const metricName = useMemo(
    () => (plot.metricName !== promQLMetric ? plot.metricName : data.nameMetric),
    [data.nameMetric, plot.metricName]
  );

  const what = useMemo(
    () =>
      plot.metricName === promQLMetric
        ? data.whats.map((qw) => whatToWhatDesc(qw)).join(', ')
        : plot.what.map((qw) => whatToWhatDesc(qw)).join(', '),
    [plot.metricName, plot.what, data.whats]
  );

  const title = useMemo(
    () => plot.customName || `${metricName}${!!what && ': ' + what}`,
    [metricName, plot.customName, what]
  );

  const onOpen = useCallback(() => {
    setOpen(true);
  }, []);

  const onClose = useCallback(() => {
    setOpen(false);
  }, []);

  useEffect(() => {
    active.current = indexPlot === tabNum;
  }, [indexPlot, tabNum]);

  useEffect(() => {
    const onTouchToggle = (event: Event) => {
      let t = event.target as HTMLElement;
      while (t.parentElement && !(t === touchToggle.current || t === sub.current)) {
        t = t.parentElement;
      }
      if (t === touchToggle.current && active.current) {
        setOpen((s) => !s);
        event.preventDefault();
      } else if (t !== sub.current) {
        setOpen(false);
      }
    };
    document.addEventListener('touchstart', onTouchToggle, { passive: false });
    return () => {
      document.removeEventListener('touchstart', onTouchToggle);
    };
  }, []);

  return (
    <li
      className={cn('position-relative', css.plotItem, indexPlot === tabNum && isView && css.activePlotItem)}
      onMouseOver={onOpen}
      onMouseOut={onClose}
      onClick={onClose}
    >
      <PlotLink
        className={cn('nav-link', !data.error403 && ['p-0', css.preview])}
        indexPlot={indexPlot}
        title={title}
        ref={touchToggle}
      >
        {!!data.error403 && <SVGXSquare className={css.icon} />}
        {!!preview && !data.error403 && <img alt={title} src={preview} className="w-100 h-100" />}
        {(!preview || numQueries > 0) && !data.error403 && !data.error && (
          <div className="position-absolute top-50 start-50 translate-middle show-delay">
            <div className="spinner-white-bg spinner-border spinner-border-sm" role="status" aria-hidden="true"></div>
          </div>
        )}
      </PlotLink>

      <ul hidden={!open} className={cn(`nav d-flex flex-column position-absolute start-100 top-0`, css.sub)} ref={sub}>
        <li className={cn('nav-item d-flex flex-row', css.bigPreview)}>
          <PlotLink
            className="nav-link text-nowrap flex-grow-1 text-body fw-bold font-monospace text-decoration-none d-flex flex-row w-0"
            indexPlot={indexPlot}
            title={title}
          >
            {plot.customName || (
              <>
                <span className="text-truncate">{metricName}</span>
                {!!what && (
                  <>
                    <span className="pe-1">:</span>
                    <span className="text-secondary text-truncate">{what}</span>
                  </>
                )}
              </>
            )}
          </PlotLink>
          <span role="button" title="Remove" className="d-block p-2 text-body" onClick={onRemovePlot}>
            <SVGTrash />
          </span>
        </li>
        {!!preview && !data.error403 && (
          <li className="nav-item p-1">
            <img alt={title} src={preview} className={css.bigPreview} />
          </li>
        )}
      </ul>
    </li>
  );
};
