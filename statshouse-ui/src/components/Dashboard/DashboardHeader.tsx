// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useMemo } from 'react';
import { PlotControlFrom, PlotControlTimeShifts, PlotControlTo, PlotNavigate } from '../Plot';
import {
  selectorDashboardId,
  selectorDashboardLayoutEdit,
  selectorDisabledLive,
  selectorParams,
  selectorSetBaseRange,
  selectorSetDashboardLayoutEdit,
  selectorSetTimeRange,
  selectorTimeRange,
  setLiveMode,
  toggleEnableTVMode,
  useLiveModeStore,
  useStore,
  useTVModeStore,
} from '../../store';
import { ReactComponent as SVGGearFill } from 'bootstrap-icons/icons/gear-fill.svg';
import { ReactComponent as SVGArrowCounterclockwise } from 'bootstrap-icons/icons/arrow-counterclockwise.svg';
import { ReactComponent as SVGFullscreen } from 'bootstrap-icons/icons/fullscreen.svg';
import { ReactComponent as SVGFullscreenExit } from 'bootstrap-icons/icons/fullscreen-exit.svg';
import { NavLink } from 'react-router-dom';
import { produce } from 'immer';
import { encodeParams } from '../../url/queryParams';
import { Button, ToggleButton } from '../UI';
import { fixMessageTrouble } from '../../url/fixMessageTrouble';

export type DashboardHeaderProps = {};
export const DashboardHeader: React.FC<DashboardHeaderProps> = () => {
  const dashboardId = useStore(selectorDashboardId);

  const tvMode = useTVModeStore((state) => state.enable);

  // const [] =

  const timeRange = useStore(selectorTimeRange);
  const setTimeRange = useStore(selectorSetTimeRange);

  const setBaseRange = useStore(selectorSetBaseRange);

  const live = useLiveModeStore((s) => s.live);
  const disabledLive = useStore(selectorDisabledLive);

  const dashboardLayoutEdit = useStore(selectorDashboardLayoutEdit);
  const setDashboardLayoutEdit = useStore(selectorSetDashboardLayoutEdit);

  const params = useStore(selectorParams);

  const copyLink = useMemo(() => {
    const search = encodeParams(
      produce(params, (p) => {
        if (p.dashboard?.dashboard_id) {
          p.dashboard.dashboard_id = undefined;
        }
      })
    );
    return `${document.location.protocol}//${document.location.host}${document.location.pathname}?${fixMessageTrouble(
      new URLSearchParams(search).toString()
    )}`;
  }, [params]);

  return (
    <div className="d-flex flex-row flex-wrap my-3 container-xl">
      <div className="me-3 mb-2">
        <PlotNavigate
          className="btn-group-sm"
          setTimeRange={setTimeRange}
          live={live}
          link={copyLink}
          outerLink={copyLink}
          setLive={setLiveMode}
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
        <div className="ms-3 mb-2">
          <PlotControlTo
            className="btn-group-sm"
            classNameInput="form-control-sm"
            timeRange={timeRange}
            setTimeRange={setTimeRange}
          />
        </div>
        <div className="ms-3 mb-2">
          <PlotControlTimeShifts />
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
        {!!dashboardId && (
          <div className="ms-2 mb-2">
            <NavLink to={`/view?id=${dashboardId}`} end>
              <Button type="button" className="btn btn-sm btn-outline-primary" title="Reset dashboard to saved state">
                <SVGArrowCounterclockwise />
              </Button>
            </NavLink>
          </div>
        )}
        <div className="ms-2 mb-2">
          <ToggleButton
            className="btn btn-outline-primary btn-sm"
            checked={tvMode}
            onChange={toggleEnableTVMode}
            title={tvMode ? 'TV mode Off' : 'TV mode On'}
          >
            {tvMode ? <SVGFullscreenExit /> : <SVGFullscreen />}
          </ToggleButton>
        </div>
      </div>
    </div>
  );
};
