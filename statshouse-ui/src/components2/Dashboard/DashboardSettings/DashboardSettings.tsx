import { memo } from 'react';
import { DashboardInfo } from './DashboardInfo';
import { DashboardVariable } from './DashboardVariable';

export type DashboardSettingsProps = {
  className?: string;
};

export const DashboardSettings = memo(function DashboardSettings() {
  return (
    <div className="w-max-720 mx-auto">
      <div className="">
        <div className="mb-4">
          <DashboardInfo />
        </div>
        <div className="mb-4">{<DashboardVariable />}</div>
      </div>
    </div>
  );
});
