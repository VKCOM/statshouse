// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { Dispatch, memo, SetStateAction, useCallback, useMemo, useState } from 'react';
import { metricMeta } from '../../view/api';
import { PlotNavigate } from './PlotNavigate';
import { SetTimeRangeValue } from '../../common/TimeRange';
import { getUrlSearch, lockRange, PlotParams } from '../../common/plotQueryParams';
import produce from 'immer';
import { selectorDashboardLayoutEdit, selectorParams, useStore } from '../../store';
import cn from 'classnames';
import css from './style.module.css';
import { PlotHeaderTitle } from './PlotHeaderTitle';
import { PlotHeaderBadges } from './PlotHeaderBadges';
import { ReactComponent as SVGChevronDown } from 'bootstrap-icons/icons/chevron-down.svg';
import { ReactComponent as SVGChevronUp } from 'bootstrap-icons/icons/chevron-up.svg';

export type PlotHeaderProps = {
  indexPlot?: number;
  compact?: boolean;
  dashboard?: boolean;
  sel: PlotParams;
  meta: metricMeta;
  live: boolean;
  setParams: (nextState: React.SetStateAction<PlotParams>, replace?: boolean | undefined) => void;
  setLive: Dispatch<SetStateAction<boolean>>;
  setTimeRange: (value: SetTimeRangeValue, force?: boolean) => void;
  yLock: lockRange;
  onResetZoom?: () => void;
  onYLockChange?: (status: boolean) => void;
};
export const _PlotHeader: React.FC<PlotHeaderProps> = ({
  indexPlot = 0,
  compact,
  dashboard,
  sel,
  meta,
  onResetZoom,
  onYLockChange,
  yLock,
  live,
  setLive,
  setTimeRange,
}) => {
  const params = useStore(selectorParams);
  const dashboardLayoutEdit = useStore(selectorDashboardLayoutEdit);

  const [showTags, setShowTags] = useState(false);
  const toggleShowTags = useCallback(() => {
    setShowTags((s) => !s);
  }, []);

  const copyLink = useMemo(
    () =>
      `${document.location.protocol}//${document.location.host}${document.location.pathname}${getUrlSearch(
        produce((prev) => {
          prev.dashboard = undefined;
          prev.tabNum = 0;
          prev.plots = [prev.plots[indexPlot]].filter(Boolean);
          prev.tagSync = [];
        }),
        params,
        ''
      )}`,
    [indexPlot, params]
  );

  if (dashboard) {
    return (
      <div className={` overflow-force-wrap font-monospace fw-bold ${compact ? 'text-center' : ''}`}>
        {!compact && (
          <PlotNavigate
            className="btn-group-sm float-end ms-4 mb-2"
            setTimeRange={setTimeRange}
            onResetZoom={onResetZoom}
            onYLockChange={onYLockChange}
            live={live}
            setLive={setLive}
            yLock={yLock}
            disabledLive={!sel.useV2}
            link={copyLink}
          />
        )}
        <div
          className={cn(
            'd-flex position-relative w-100',
            !dashboardLayoutEdit && !sel.customName && !showTags && 'pe-4'
          )}
        >
          <div className="flex-grow-1 text-truncate overflow-hidden w-0 px-1 d-flex text-nowrap">
            <PlotHeaderTitle indexPlot={indexPlot} compact={compact} dashboard={dashboard} />
          </div>
          {!dashboardLayoutEdit && !sel.customName && (
            <>
              <div
                className={cn(
                  css.badge,
                  'd-flex gap-1 z-1 flex-row',
                  showTags
                    ? 'position-absolute bg-body end-0 top-0 flex-wrap align-items-end justify-content-end pt-4 p-1'
                    : 'overflow-hidden  flex-nowrap',
                  showTags ? css.badgeShow : css.badgeHide
                )}
              >
                <PlotHeaderBadges
                  indexPlot={indexPlot}
                  compact={compact}
                  dashboard={dashboard}
                  className={cn(showTags ? 'text-wrap' : 'text-nowrap')}
                />
              </div>
              <div role="button" onClick={toggleShowTags} className="z-2 px-1 position-absolute end-0 top-0">
                {showTags ? <SVGChevronUp width="12px" height="12px" /> : <SVGChevronDown width="12px" height="12px" />}
              </div>
            </>
          )}
        </div>
        {!compact && (
          /*description*/
          <small
            className="overflow-force-wrap text-secondary fw-normal font-normal flex-grow-0"
            style={{ whiteSpace: 'pre-wrap' }}
          >
            {meta.description}
          </small>
        )}
      </div>
    );
  }

  return (
    <div>
      {/*title + controls*/}
      <div className={`d-flex flex-grow-1 flex-wrap justify-content-${compact ? 'around' : 'between'}`}>
        {/*title*/}
        <h6
          className={`d-flex flex-wrap justify-content-center align-items-center overflow-force-wrap font-monospace fw-bold me-3 flex-grow-1 mb-1`}
        >
          <PlotHeaderTitle indexPlot={indexPlot} compact={compact} dashboard={dashboard} />
          <PlotHeaderBadges indexPlot={indexPlot} compact={compact} dashboard={dashboard} />
        </h6>
        {!compact && (
          <PlotNavigate
            className="btn-group-sm mb-1"
            setTimeRange={setTimeRange}
            onResetZoom={onResetZoom}
            onYLockChange={onYLockChange}
            live={live}
            setLive={setLive}
            yLock={yLock}
            disabledLive={!sel.useV2}
            link={copyLink}
          />
        )}
      </div>
      {!compact && (
        /*description*/
        <small className="overflow-force-wrap text-secondary flex-grow-0" style={{ whiteSpace: 'pre-wrap' }}>
          {meta.description}
        </small>
      )}
    </div>
  );
};

export const PlotHeader = memo(_PlotHeader);
