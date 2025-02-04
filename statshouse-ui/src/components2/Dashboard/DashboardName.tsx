// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { memo, useMemo, useState } from 'react';
import { Tooltip } from '@/components/UI';
import { DashboardNameTitle } from './DashboardNameTitle';
import { useStatsHouseShallow } from '@/store2';
import css from '../style.module.css';
import { MarkdownRender } from '@/components2/Plot/PlotView/MarkdownRender';
import { produce } from 'immer';
import { StickyTop } from '../StickyTop';
import { SaveButton } from '../SaveButton';
import { HistoryDashboardLabel } from '../HistoryDashboardLabel';

export const DashboardName = memo(function DashboardName() {
  const {
    dashboardName,
    dashboardDescription,
    saveParams,
    saveDashboard,
    setParams,
    dashboardVersion,
    dashboardCurrentVersion,
  } = useStatsHouseShallow(
    ({
      params: { dashboardName, dashboardDescription, dashboardVersion, dashboardCurrentVersion },
      saveParams,
      saveDashboard,
      setParams,
    }) => ({
      dashboardName,
      dashboardDescription,
      saveParams,
      saveDashboard,
      setParams,
      dashboardVersion,
      dashboardCurrentVersion,
    })
  );

  const [dropdown, setDropdown] = useState(false);

  const isHistoricalDashboard = useMemo(
    () => !!dashboardVersion && !!dashboardCurrentVersion && dashboardCurrentVersion !== dashboardVersion,
    [dashboardVersion, dashboardCurrentVersion]
  );

  if (!dashboardName) {
    return null;
  }

  const onDashboardSave = async (copy?: boolean) => {
    const dashResponse = await saveDashboard(copy);
    if (dashResponse) {
      setParams(
        produce((params) => {
          params.dashboardCurrentVersion = undefined;
        })
      );
      setDropdown(false);
    }
  };

  const isDashNamesEqual = dashboardName === saveParams.dashboardName;

  return (
    <StickyTop>
      <div className="container-xl d-flex">
        <Tooltip
          className="d-flex flex-row gap-2 w-75 my-auto"
          title={<DashboardNameTitle name={dashboardName} description={dashboardDescription} />}
          hover
          horizontal="left"
        >
          <div className="overflow-hidden text-truncate">
            {dashboardName}
            {!!dashboardDescription && ':'}
          </div>
          {!!dashboardDescription && (
            <div className="text-secondary flex-grow-1 w-0 overflow-hidden">
              <MarkdownRender
                className={css.markdown}
                allowedElements={['p', 'a']}
                components={{
                  p: ({ node, ...props }) => <span {...props} />,
                }}
                unwrapDisallowed
              >
                {dashboardDescription}
              </MarkdownRender>
            </div>
          )}
        </Tooltip>
        {isHistoricalDashboard && (
          <div className="d-flex flex-row gap-2 ms-auto">
            <HistoryDashboardLabel />
            <SaveButton
              onSave={onDashboardSave}
              isNamesEqual={isDashNamesEqual}
              dropdown={dropdown}
              setDropdown={setDropdown}
            />
          </div>
        )}
      </div>
    </StickyTop>
  );
});
