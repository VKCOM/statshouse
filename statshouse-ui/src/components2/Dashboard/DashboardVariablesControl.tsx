import React, { memo } from 'react';
import { useStatsHouseShallow } from '../../store2';
import { PlotControlFilterVariable } from '../Plot/PlotControl/PlotControlFilterVariable';
import cn from 'classnames';

export type DashboardVariablesControlProps = {
  className?: string;
};

export function _DashboardVariablesControl({ className }: DashboardVariablesControlProps) {
  const { orderVariables } = useStatsHouseShallow(({ params: { orderVariables } }) => ({ orderVariables }));
  return (
    <div className={cn(className)}>
      <div className="row">
        {orderVariables.map((variableKey) => (
          <PlotControlFilterVariable
            className={'col-12 col-lg-3 col-md-6 mb-2'}
            key={variableKey}
            variableKey={variableKey}
          />
        ))}
      </div>
    </div>
  );
}
export const DashboardVariablesControl = memo(_DashboardVariablesControl);
