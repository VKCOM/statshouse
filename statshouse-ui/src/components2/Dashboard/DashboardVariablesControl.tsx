// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { memo } from 'react';
import { useStatsHouseShallow } from '@/store2';
import { PlotControlFilterVariable } from '../Plot/PlotControl/PlotControlFilterVariable';
import cn from 'classnames';
import { DashboardVariablesBadgeByKey } from './DashboardVariablesBadgeByKey';

export type DashboardVariablesControlProps = {
  className?: string;
};

export const DashboardVariablesControl = memo(function DashboardVariablesControl({
  className,
}: DashboardVariablesControlProps) {
  const { orderVariables, isEmbed } = useStatsHouseShallow(({ params: { orderVariables }, isEmbed }) => ({
    orderVariables,
    isEmbed,
  }));
  return (
    <div className={cn(className)}>
      <div className="row">
        {orderVariables.map((variableKey) =>
          !isEmbed ? (
            <PlotControlFilterVariable
              className={'col-12 col-lg-3 col-md-6 mb-2'}
              key={variableKey}
              variableKey={variableKey}
            />
          ) : (
            <DashboardVariablesBadgeByKey
              className={'col-12 col-lg-3 col-md-6 mt-2 align-items-start'}
              key={variableKey}
              variableKey={variableKey}
            />
          )
        )}
      </div>
    </div>
  );
});
