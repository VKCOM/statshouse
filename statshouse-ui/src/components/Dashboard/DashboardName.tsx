import React from 'react';
import { selectorParams, useStore } from 'store';
import { useWindowSize } from 'hooks/useWindowSize';
import cn from 'classnames';
import { Tooltip } from 'components/UI';
import { DashboardNameTitle } from './DashboardNameTitle';

export type DashboardNameProps = {};

export function DashboardName() {
  const params = useStore(selectorParams);
  const scrollY = useWindowSize((s) => s.scrollY > 16);

  if (!params.dashboard?.name) {
    return null;
  }
  return (
    <div className={cn('sticky-top mt-2 bg-body', scrollY && 'shadow-sm small')}>
      <Tooltip
        className="container-xl d-flex flex-row gap-2"
        title={<DashboardNameTitle name={params.dashboard.name} description={params.dashboard.description} />}
        hover
        horizontal="left"
      >
        <div>
          {params.dashboard.name}
          {!!params.dashboard.description && ':'}
        </div>
        {!!params.dashboard.description && (
          <div className="text-secondary flex-grow-1 w-0 overflow-hidden text-truncate">
            {params.dashboard.description}
          </div>
        )}
      </Tooltip>
    </div>
  );
}
