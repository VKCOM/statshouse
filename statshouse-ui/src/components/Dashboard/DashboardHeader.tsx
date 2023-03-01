// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback } from 'react';
import { PlotControlFrom, PlotControlTimeShifts, PlotControlTo, PlotNavigate } from '../Plot';
import {
  selectorDashboardId,
  selectorDashboardLayoutEdit,
  selectorDisabledLive,
  selectorLiveMode,
  selectorSetBaseRange,
  selectorSetDashboardLayoutEdit,
  selectorSetLiveMode,
  selectorSetTimeRange,
  selectorTimeRange,
  useStore,
} from '../../store';
import { ReactComponent as SVGGearFill } from 'bootstrap-icons/icons/gear-fill.svg';
import { ReactComponent as SVGArrowCounterclockwise } from 'bootstrap-icons/icons/arrow-counterclockwise.svg';
import { NavLink } from 'react-router-dom';

import css from './style.module.css';
import cn from 'classnames';

export type DashboardHeaderProps = {};
export const DashboardHeader: React.FC<DashboardHeaderProps> = () => {
  const dashboardId = useStore(selectorDashboardId);

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
    <div className={cn('d-flex flex-row flex-wrap mb-3', css.margin)}>
      <div className="me-4 mb-2">
        <PlotNavigate
          className="btn-group-sm"
          setTimeRange={setTimeRange}
          live={live}
          setLive={setLive}
          disabledLive={disabledLive}
        />
      </div>
      <div className="d-flex flex-row flex-wrap flex-grow-1 align-items-end justify-content-end">
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
          <label className="btn btn-outline-primary btn-sm" htmlFor="dashboard-layout" title="Edit dashboard">
            <SVGGearFill />
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
