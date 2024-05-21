import React from 'react';
import { LeftMenu } from 'components2';
import { usePlotsInfoStore, useUserStore } from 'store2';
import { setDevEnabled, setTheme, useStoreDev, useThemeStore } from '../../store';

const toggleDevEnabled = () => {
  setDevEnabled((s) => !s);
};

export function LeftMenuWidget() {
  const { plotsInfo, viewOrderPlot, dashboardLink, tabNum, addLink } = usePlotsInfoStore();
  const user = useUserStore();
  const devEnabled = useStoreDev((s) => s.enabled);
  const theme = useThemeStore((s) => s.theme);
  return (
    <LeftMenu
      plots={plotsInfo}
      orderPlot={viewOrderPlot}
      user={user}
      devEnabled={devEnabled}
      toggleDevEnabled={toggleDevEnabled}
      dashboardLink={dashboardLink}
      theme={theme}
      setTheme={setTheme}
      tabNum={tabNum}
      addLink={addLink}
    />
  );
}
