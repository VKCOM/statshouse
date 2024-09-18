// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { Button } from 'components/UI';
import { useStatsHouseShallow } from 'store2';
import { useGlobalLoader } from 'store2/plotQueryStore';

export type DashboardInfoProps = {
  className?: string;
};

export function DashboardInfo({ className }: DashboardInfoProps) {
  const globalLoader = useGlobalLoader();
  const { dashboardName, dashboardDescription, isDashboard, removeDashboard, setDashboardLayoutEdit, setParams } =
    useStatsHouseShallow(
      ({
        params: { dashboardId, dashboardName, dashboardDescription },
        removeDashboard,
        setDashboardLayoutEdit,
        setParams,
      }) => ({
        dashboardName,
        dashboardDescription,
        isDashboard: dashboardId != null,
        removeDashboard,
        setDashboardLayoutEdit,
        setParams,
      })
    );

  const navigate = useNavigate();

  const inputName = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const value = e.target.value;
      setParams((p) => {
        p.dashboardName = value;
      });
    },
    [setParams]
  );
  const inputDescription = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const value = e.target.value;
      setParams((params) => {
        params.dashboardDescription = value;
      });
    },
    [setParams]
  );

  const onRemoveDashboard = useCallback(
    (event: React.MouseEvent) => {
      if (isDashboard && window.confirm(`Remove dashboard ${dashboardName}?`)) {
        removeDashboard().then(() => {
          // setDashboardLayoutEdit(false);
          // setParams((params) => {
          //   params.dashboardId = undefined;
          // }, true);
          navigate('/dash-list');
        });
      }
      event.preventDefault();
    },
    [dashboardName, isDashboard, navigate, removeDashboard]
  );

  return (
    <div className="card border-0">
      <div className="card-body p-2">
        <h5 className="card-title">Dashboard Info</h5>
        <div className="card-text">
          <div className="mb-2 row">
            <label htmlFor="dashboard-input-name" className="col-form-label col-sm-2">
              Name
            </label>
            <div className="col-sm-10">
              <input
                id="dashboard-input-name"
                type="text"
                className="form-control"
                aria-label="Name"
                defaultValue={dashboardName ?? ''}
                onInput={inputName}
              />
            </div>
          </div>
          <div className="mb-2 row">
            <label htmlFor="dashboard-input-description" className="col-form-label col-sm-2">
              Description
            </label>
            <div className="col-sm-10">
              <input
                id="dashboard-input-description"
                type="text"
                className="form-control"
                aria-label="Name"
                defaultValue={dashboardDescription ?? ''}
                onInput={inputDescription}
              />
            </div>
          </div>
          <div className="d-flex flex-row justify-content-end">
            {isDashboard && (
              <Button
                type="button"
                className="btn btn-outline-danger ms-2"
                onClick={onRemoveDashboard}
                disabled={globalLoader}
              >
                Remove
              </Button>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
