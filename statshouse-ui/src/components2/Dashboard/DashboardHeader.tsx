// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { memo } from 'react';
import { PlotNavigate } from '../Plot';
import { PlotControlFrom } from '../Plot/PlotControl/PlotControlFrom';
import { PlotControlTo } from '../Plot/PlotControl/PlotControlTo';
import { PlotControlGlobalTimeShifts } from '../Plot/PlotControl/PlotControlGlobalTimeShifts';
import { useStatsHouseShallow, viewPath } from '@/store2';
import { NavLink } from 'react-router-dom';
import { Button, ToggleButton } from '@/components/UI';
import { ReactComponent as SVGArrowCounterclockwise } from 'bootstrap-icons/icons/arrow-counterclockwise.svg';
import { ReactComponent as SVGGearFill } from 'bootstrap-icons/icons/gear-fill.svg';
import { ButtonToggleTvMode } from '../TvMode';

export const DashboardHeader = memo(function DashboardHeader() {
  const { isSaveDashboard, dashboardId, dashboardLayoutEdit, setDashboardLayoutEdit } = useStatsHouseShallow(
    ({ params: { dashboardId }, dashboardLayoutEdit, setDashboardLayoutEdit }) => ({
      isSaveDashboard: dashboardId != null,
      dashboardId,
      dashboardLayoutEdit,
      setDashboardLayoutEdit,
    })
  );
  return (
    <div className="d-flex flex-row flex-wrap my-3 container-xl">
      <div className="me-3 mb-2">
        <PlotNavigate className="btn-group-sm" />
      </div>
      <div className="d-flex flex-row flex-wrap flex-grow-1 align-items-end justify-content-end">
        <div className="flex-grow-1"></div>
        <div className="mb-2">
          <PlotControlFrom className="input-group-sm" />
        </div>
        <div className="ms-3 mb-2">
          <PlotControlTo className="input-group-sm" />
        </div>
        <div className="ms-3 mb-2">
          <PlotControlGlobalTimeShifts />
        </div>
        <div className="ms-3 mb-2">
          <ToggleButton
            className="btn btn-outline-primary btn-sm"
            checked={dashboardLayoutEdit}
            onChange={setDashboardLayoutEdit}
            title="Edit dashboard"
          >
            <SVGGearFill />
          </ToggleButton>
        </div>
        {!!isSaveDashboard && (
          <div className="ms-2 mb-2">
            <NavLink reloadDocument to={{ pathname: viewPath[0], search: `?id=${dashboardId}` }} end>
              <Button type="button" className="btn btn-sm btn-outline-primary" title="Reset dashboard to saved state">
                <SVGArrowCounterclockwise />
              </Button>
            </NavLink>
          </div>
        )}
        <div className="ms-2 mb-2">
          <ButtonToggleTvMode className="btn-sm" />
        </div>
      </div>
    </div>
  );
});
