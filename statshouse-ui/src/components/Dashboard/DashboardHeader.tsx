// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useMemo } from 'react';
import { PlotControlFrom, PlotControlTimeShifts, PlotControlTo, PlotNavigate } from '../Plot';
import { DashboardTagControl } from '../DashboardTagControl';
import cn from 'classnames';
import {
  selectorDashboardId,
  selectorDashboardLayoutEdit,
  selectorDisabledLive,
  selectorLiveMode,
  selectorParamsTagSync,
  selectorSetBaseRange,
  selectorSetDashboardLayoutEdit,
  selectorSetLiveMode,
  selectorSetTimeRange,
  selectorTimeRange,
  useStore,
} from '../../store';
import { ReactComponent as SVGColumnsGap } from 'bootstrap-icons/icons/columns-gap.svg';
import { ReactComponent as SVGArrowCounterclockwise } from 'bootstrap-icons/icons/arrow-counterclockwise.svg';
import { NavLink } from 'react-router-dom';

export type DashboardHeaderProps = {};
export const DashboardHeader: React.FC<DashboardHeaderProps> = () => {
  const dashboardId = useStore(selectorDashboardId);
  const tagsSync = useStore(selectorParamsTagSync);
  const showSyncPanel = useMemo(() => tagsSync.some((group) => group.some((s) => s !== null)), [tagsSync]);

  const timeRange = useStore(selectorTimeRange);
  const setTimeRange = useStore(selectorSetTimeRange);

  const setBaseRange = useStore(selectorSetBaseRange);

  const live = useStore(selectorLiveMode);
  const setLive = useStore(selectorSetLiveMode);
  const disabledLive = useStore(selectorDisabledLive);

  const dashboardLayoutEdit = useStore(selectorDashboardLayoutEdit);
  const setDashboardLayoutEdit = useStore(selectorSetDashboardLayoutEdit);

  const onDashboardLayoutEdit = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      setDashboardLayoutEdit(e.currentTarget.checked);
    },
    [setDashboardLayoutEdit]
  );

  return (
    <div className="d-flex flex-row flex-wrap mb-3" style={{ marginLeft: 'var(--plot-margin)' }}>
      <div className="me-4 mb-2 order-0">
        <PlotNavigate
          className="btn-group-sm"
          setTimeRange={setTimeRange}
          live={live}
          setLive={setLive}
          disabledLive={disabledLive}
        />
      </div>
      {showSyncPanel && (
        <DashboardTagControl
          className={cn(
            'd-flex flex-grow-1 flex-row gap-3 flex-wrap',
            tagsSync.length > 1
              ? 'order-4 col-12 justify-content-start'
              : 'order-4 col-12 order-lg-2 col-lg-3 justify-content-center'
          )}
        />
      )}
      <div className="d-flex flex-row flex-wrap flex-grow-1 align-items-end justify-content-end order-3">
        <div className="flex-grow-1"></div>
        <div className="mb-2">
          <PlotControlFrom
            className="btn-group-sm"
            classNameSelect="form-select-sm"
            timeRange={timeRange}
            setTimeRange={setTimeRange}
            setBaseRange={setBaseRange}
          />
        </div>
        <div className="ms-4 mb-2">
          <PlotControlTo
            className="btn-group-sm"
            classNameInput="form-control-sm"
            timeRange={timeRange}
            setTimeRange={setTimeRange}
          />
        </div>
        <div className="ms-4 mb-2">
          <PlotControlTimeShifts />
        </div>
        <div className="ms-4 mb-2">
          <input
            type="checkbox"
            className="btn-check"
            id="dashboard-layout"
            autoComplete="off"
            checked={dashboardLayoutEdit}
            onChange={onDashboardLayoutEdit}
          />
          <label className="btn btn-outline-primary btn-sm" htmlFor="dashboard-layout" title="Edit dashboard layout">
            <SVGColumnsGap />
          </label>
        </div>
        {!!dashboardId && (
          <div className="ms-2 mb-2">
            <NavLink to={`/view?id=${dashboardId}`} end>
              <button type="button" className="btn btn-sm btn-outline-primary" title="Reset dashboard to saved state">
                <SVGArrowCounterclockwise />
              </button>
            </NavLink>
          </div>
        )}
      </div>
    </div>
  );
};
