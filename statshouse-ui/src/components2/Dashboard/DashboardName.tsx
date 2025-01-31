// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { memo } from 'react';
import { useWindowSize } from '@/hooks/useWindowSize';
import cn from 'classnames';
import { Tooltip } from '@/components/UI';
import { DashboardNameTitle } from './DashboardNameTitle';
import { useStatsHouseShallow } from '@/store2';
import css from '../style.module.css';
import { MarkdownRender } from '@/components2/Plot/PlotView/MarkdownRender';

export const DashboardName = memo(function DashboardName() {
  const { dashboardName, dashboardDescription } = useStatsHouseShallow(
    ({ params: { dashboardName, dashboardDescription } }) => ({ dashboardName, dashboardDescription })
  );
  const scrollY = useWindowSize((s) => s.scrollY > 16);

  if (!dashboardName) {
    return null;
  }
  return (
    <div className={cn('sticky-top mt-2 bg-body', scrollY && 'shadow-sm small')}>
      <Tooltip
        className="container-xl d-flex flex-row gap-2"
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
    </div>
  );
});
